
#include <vector>
#include <string>
#include <cstring>
#include <iostream>

#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) : iter(input), cond(condition) {
    // Iterator is just an iterator over some tuples, cannot use regular rbfm scan to do stuff, must
    // use this scan iterator since it may have some kinds of other conditions we are unaware of
    // Must use condition to re-implement a getNextTuple() function
    // Iterator class may either be an index scan or table scan, doesnt really matter
    /*iter = input;
    cond.lhsAttr = condition.lhsAttr;
    cond.op = condition.op;
    cond.bRhsIsAttr = condition.bRhsIsAttr;
    if(cond.bRhsIsAttr) {
        cond.rhsAttr = condition.rhsAttr;
    }
    else {
        cond.rhsValue = condition.rhsValue;
    }*/

    vector<Attribute> attributes;
    iter->getAttributes(attributes);
    bool found_attr = false;
    int i;
    for (i = 0; i < (int) attributes.size(); i += 1) {
        if (attributes[i].name.compare(cond.lhsAttr) == 0) {
            found_attr = true;
            break;
        }
    }

    if (!found_attr) {
        //attribute to compare against does not exist in our attrs, so fail
        error = FILTER_ATTR_NT_EXIST;
    }
    else {
        compare_attr_index = i;
        compare_attr = attributes[i];
    }

    if (cond.bRhsIsAttr || cond.rhsValue.type != attributes[i].type) {
        error = FILTER_BAD_COND;
    }

    error = SUCCESS;
}

Filter::~Filter() {}

int Filter::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool Filter::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

RC Filter::getNextTuple(void* data) {
    if (iter == NULL)
        return FILTER_NT_INIT;

    // get the next tuple out of the iterator and do the comparison
    void* cur_data = calloc(PAGE_SIZE, 1);
    RC rc;
    vector<Attribute> attributes;
    iter->getAttributes(attributes);
    bool valid = false;
    while ((rc = iter->getNextTuple(cur_data)) == SUCCESS) {
        // TODO keep going until we have data that is valid for the 
        // new condition we are applying
        
        // int i;
        // bool found_attr = false;
        // for (i = 0; i < attributes.size(); i += 1) {
        //     if (attributes[i].name.compare(cond.lhsAttr) == 0) {
        //         found_attr = true;
        //         break;
        //     }
        // }
        // if (!found_attr) {
        //     //attribute to compare against does not exist in our attrs, so fail
        //     return FILTER_ATTR_NT_EXIST;
        // }
        // if (cond.bRhsIsAttr || cond.rhsValue.type != attributes[i].type) {
        //     return FILTER_BAD_COND;
        // }
        int nullIndicatorSize = getNullIndicatorSize(attributes.size());
        char nullIndicator[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);
        memcpy(nullIndicator, (char*) cur_data, nullIndicatorSize);

        // Offset into *data. Start just after null indicator
        unsigned recordSize = nullIndicatorSize;
        // Running count of size. Initialize to size of header
        // unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;
        // count up size of the record (amount of bytes to copy into *data)
        // when we reach the index of the attribute to be compared, do the comparison
        for (unsigned i = 0; i < attributes.size(); i += 1)
        {
            if (i == (unsigned) compare_attr_index) {
                //then offset of the record is already what we need it to be
                if (compare_attr.type == TypeInt) {
                    int32_t recordInt;
                    memcpy(&recordInt, (char*)cur_data + recordSize, INT_SIZE);
                    /* cerr << "cond.rhsValue: " << *(int *)cond.rhsValue.data << endl; */
                    valid = checkScanCondition(recordInt, cond.op, cond.rhsValue.data);
                    /* cerr << "int case set the valid flag to: " << valid << endl; */
                }
                else if (compare_attr.type == TypeReal) {
                    float recordReal;
                    memcpy(&recordReal, (char*)cur_data + recordSize, REAL_SIZE);
                    valid = checkScanCondition(recordReal, cond.op, cond.rhsValue.data);
                    /* cerr << "float case set the valid flag to: " << valid << endl; */
                }
                else {
                    // varchar case
                    uint32_t varcharSize;
                    memcpy(&varcharSize, (char*)cur_data + recordSize, VARCHAR_LENGTH_SIZE);
                    char recordString[varcharSize + 1];
                    memcpy(recordString, (char*)cur_data + recordSize + VARCHAR_LENGTH_SIZE, varcharSize);
                    recordString[varcharSize] = '\0';
                    // TODO: Verify we want to pass a pointer to the rhsValue.
                    valid = checkScanCondition(recordString, cond.op, cond.rhsValue.data);
                    /* cerr << "varchar case set the valid flag to: " << valid << endl; */
                }
            }
            // Skip null fields
            if (fieldIsNull(nullIndicator, i)) {
                /* cerr << "found null field." << endl; */
                continue;
            }
          
            switch (attributes[i].type)
            {
                case TypeInt:
                    // size += INT_SIZE;
                    recordSize += INT_SIZE;
                break;
                case TypeReal:
                    // size += REAL_SIZE;
                    recordSize += REAL_SIZE;
                break;
                case TypeVarChar:
                    uint32_t varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, (char*) cur_data + recordSize, VARCHAR_LENGTH_SIZE);
                    // size += varcharSize;
                    recordSize += varcharSize + VARCHAR_LENGTH_SIZE;
                break;
            }
        }

        if (valid) {
            /* cerr << "found a valid tuple!" << endl; */
            memcpy(data, cur_data, recordSize);
            valid = false;
            return SUCCESS;
        }
    }

    if (rc == QE_EOF) {
        return QE_EOF;
    }
    else {
        return rc;
    }
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    // im not sure which attributes im getting
    // its either those of the table that im iterating over
    // or its the attributes that im returning when i return each tuple
    iter->getAttributes(attrs);
}

Project::Project(Iterator* input, const vector<string> &attrNames) {
    //iterator is just an iterator over some tuples, cannot use regular rbfm scan to do stuff, must
    //  use this scan iterator since it may have some kinds of other conditions we are unaware of
    // must use condition to re-implement a getNextTuple() function
    // iterator class may either be an index scan or table scan, doesnt really matter
    iter = input;
    names.clear();
    for (unsigned i = 0; i < attrNames.size(); i++)
        names.push_back(attrNames[i]);

    vector<Attribute> attrs;
    input->getAttributes(attrs);
    if(attrNames.size() > attrs.size()) {
        error = PRJCT_BAD_ATTR_COND;
        return;
    }
  
    for (unsigned i = 0; i < attrNames.size(); i += 1) {
        for (unsigned j = 0; j < attrs.size(); j += 1) {
            if (attrNames[i].compare(attrs[j].name) == 0) {
                // the names match
                Attribute attr;
                attr.name = attrs[j].name;
                attr.type = attrs[j].type;
                attr.length = attrs[j].length;
                projection_attributes.push_back(attr);
            }
        }
    }
  
    if (projection_attributes.size() != attrNames.size()) {
        // then not all attributes found a name that matched, 
        // therefore some attribute must have been passed that does not exist
        error = PRJCT_BAD_ATTR_COND;
    } else {
        error = SUCCESS;
    }
}

RC Project::getNextTuple(void* data) {
    if (iter == NULL)
        return PRJCT_NT_INIT;
    
    //get the next tuple out of the iterator and do the comparison
    void* cur_data = calloc(PAGE_SIZE, 1);
    RC rc;
    vector<Attribute> attrs;
    iter->getAttributes(attrs);
    while ((rc = iter->getNextTuple(cur_data)) == SUCCESS) {
        // retrieve the tuple
        // let us assume we can return the data in the exact order it was given to us, and we only need to cut out data
        int given_null_size = getNullIndicatorSize(attrs.size());
        int our_null_size = getNullIndicatorSize(projection_attributes.size());
        char* nullIndicator = (char*)calloc(given_null_size, 1);
        memcpy(nullIndicator, cur_data, given_null_size);
        char* nulls = (char*)calloc(our_null_size, 1);
        int output_offset = our_null_size;
        int nulls_index = 0;
        if (given_null_size != our_null_size) {
            // if the null size of the attributes of the vector are not the same as the null size of the 
            // TODO:
            cerr << "getNextTuple: NotImplemented." << endl;
        }
        else {
            // int offset = 0;
            // // memcpy(data, cur_data, our_null_size);  // we cant do this, we have to adjust no matter what
            // offset += given_null_size;
            // output_offset = given_null_size;
            //time to add all of our data into the buffer
            for (unsigned i = 0; i < projection_attributes.size(); i += 1) {
                int cur_field = 0;
                int offset = given_null_size;
                while(projection_attributes[i].name.compare(attrs[cur_field].name) != 0) {
                    attrs[cur_field];
                    if (fieldIsNull(nullIndicator, cur_field))
                        continue;

                    switch (attrs[cur_field].type)
                    {
                        case TypeInt:
                            offset += INT_SIZE;
                            break;
                        case TypeReal:
                            offset += REAL_SIZE;
                            break;
                        case TypeVarChar:
                            uint32_t varcharSize;
                            // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                            memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                            // size += varcharSize;
                            offset += varcharSize + VARCHAR_LENGTH_SIZE;
                            break;
                    }
                    
                    cur_field += 1;
                }
                // we are now at a field we need
                if (fieldIsNull(nullIndicator, cur_field)) {
                    // if the field we want is null, then we have to write a null val into OUR nulls indicator
                    setFieldToNull(nulls, nulls_index);
                    //since we call continue, we need to increment this right here immediately
                    nulls_index += 1;
                    continue;
                }

                switch (projection_attributes[cur_field].type)
                {
                    case TypeInt:
                        memcpy((char*)data + output_offset, (char*)cur_data + offset, INT_SIZE);
                        offset += INT_SIZE;
                        output_offset += INT_SIZE;
                    break;
                    case TypeReal:
                        memcpy((char*)data + output_offset, (char*)cur_data + offset, REAL_SIZE);
                        offset += REAL_SIZE;
                        output_offset += REAL_SIZE;
                    break;
                    case TypeVarChar:
                        uint32_t varcharSize;
                        // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                        memcpy(&varcharSize, (char*)cur_data + offset, VARCHAR_LENGTH_SIZE);
                        memcpy((char*)data + output_offset, &varcharSize, VARCHAR_LENGTH_SIZE);
                        // size += varcharSize;
                        offset += VARCHAR_LENGTH_SIZE;
                        output_offset += VARCHAR_LENGTH_SIZE;
                        memcpy((char*)data + output_offset, (char*)cur_data + offset, varcharSize);
                        offset += varcharSize;
                        output_offset += varcharSize;
                    break;
                }
                cur_field += 1;
                // if we do in fact write a value in, we do not want to mark this part of our null bits as null, so increment
                nulls_index += 1;
            }
        }
        memcpy(data, nulls, our_null_size);
        free(nulls);
    }

    free(cur_data);
    return rc;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    // im not sure which attributes im getting
    // its either those of the table that im iterating over
    // or its the attributes that im returning when i return each tuple
    // I am assuming that the attributes I return here are the attributes that MY PROJECTION returns, 
    // NOT what I GET when I call getNextTuple() on the underlying iterator
    iter->getAttributes(attrs);
}
// ... the rest of your implementations go here

int Project::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool Project::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

RC Project::setFieldToNull(char *nullIndicator, int i) {
    int indicatorIndex = i / CHAR_BIT;
    uint8_t mask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));

    // int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    nullIndicator[indicatorIndex] = nullIndicator[indicatorIndex] | mask;
    return SUCCESS;
}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
        IndexScan *rightIn,          // IndexScan Iterator of input S
        const Condition &condition){   // Join condition

    left = leftIn;
    right = rightIn;
    cond = condition; 
    leftIn->getAttributes(left_attrs);
    right_attrs = rightIn->attrs;
    newLeft = true;
    outer_page_data = malloc(PAGE_SIZE);
    //check to make sure that the two conditional attrs for inner and outer are both ,
    // as well as record the indexes of each of these attrs
    bool attr_exists = false;
    for (unsigned i = 0; i < left_attrs.size(); i += 1) {
        if (left_attrs[i].name.compare(cond.lhsAttr) == 0) {
            attr_exists = true;
            left_attr_comp_index = i;
            break;
        }
    }

    if (!attr_exists) {
        error = JOIN_BAD_COND;
        return;
    }

    for (unsigned i = 0; i < right_attrs.size(); i += 1) {
        if (right_attrs[i].name.compare(cond.rhsAttr) == 0) {
            attr_exists = true;
            right_attr_comp_index = i;
            break;
        }
    }

    if (!attr_exists) {
        error  = JOIN_BAD_COND;
        return;
    }

    for (unsigned i = 0; i < left_attrs.size(); i += 1) { 
        total_attrs.push_back(left_attrs[i]);
    }

    for (unsigned i = 0; i < right_attrs.size(); i += 1) { 
        total_attrs.push_back(right_attrs[i]);
    }

    error = SUCCESS;
}

RC INLJoin::getNextTuple(void *data) {
    // start looping through left, loop through all of right, check condition
    // for each condition, concat the two and return it
    RC outer_rc;
    RC inner_rc;
    // set up getting the first tuple from left iter
    void* inner_page_data = malloc(PAGE_SIZE);
    // we do not want to get a new tuple from left each time we call get next tuple, only when we reach EOF of inner loop
    if (newLeft) {
        outer_rc = left->getNextTuple(outer_page_data);
        if (outer_rc == QE_EOF) {
            free(inner_page_data);
            return outer_rc;
        }

        if (outer_rc) {
            free(inner_page_data);
            return outer_rc;
        }
    }

    while ((inner_rc = right->getNextTuple(inner_page_data)) == SUCCESS) {
        // do comparison
        // getAttributeFromRecord(inner_page_data, 0, right_attr_comp_index, right_attrs[right_attr_comp_index].type, right_data);
        int right_comp_attr_offset = getAttributeOffset(inner_page_data, false);
        int left_comp_attr_offset = getAttributeOffset(outer_page_data, true);
      
        // get left data
        void* left_comp_data = NULL;
        switch (left_attrs[left_attr_comp_index].type)
        {
            case TypeInt:
                left_comp_data = malloc(INT_SIZE);
                memcpy(left_comp_data, (char*)outer_page_data + left_comp_attr_offset, INT_SIZE);
                break;
            case TypeReal:
                left_comp_data = malloc(REAL_SIZE);
                memcpy(left_comp_data, (char*)outer_page_data + left_comp_attr_offset, REAL_SIZE);
                break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*)outer_page_data + left_comp_attr_offset, VARCHAR_LENGTH_SIZE);
                left_comp_data = malloc(VARCHAR_LENGTH_SIZE + varcharSize);
                memcpy(left_comp_data, &varcharSize, VARCHAR_LENGTH_SIZE);
                memcpy(left_comp_data, (char*)outer_page_data + left_comp_attr_offset + VARCHAR_LENGTH_SIZE, varcharSize);
                break;
            default:
                cerr << "getNextTuple: Invalid attribute type." << endl;
        }

        // get right data
        void* right_comp_data = NULL;
        switch (left_attrs[left_attr_comp_index].type)
        {
            case TypeInt:
                right_comp_data = malloc(INT_SIZE);
                memcpy(right_comp_data, (char*)inner_page_data + right_comp_attr_offset, INT_SIZE);
                break;
            case TypeReal:
                right_comp_data = malloc(REAL_SIZE);
                memcpy(right_comp_data, (char*)inner_page_data + right_comp_attr_offset, REAL_SIZE);
                break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*)inner_page_data + right_comp_attr_offset, VARCHAR_LENGTH_SIZE);
                right_comp_data = malloc(VARCHAR_LENGTH_SIZE + varcharSize);
                memcpy(right_comp_data, &varcharSize, VARCHAR_LENGTH_SIZE);
                memcpy(right_comp_data, (char*)inner_page_data + right_comp_attr_offset + VARCHAR_LENGTH_SIZE, varcharSize);
                break;
            default:
                cerr << "getNextTuple: Invalid attribute type." << endl;
        }

        // do comparison
        bool equal = false;
        switch (left_attrs[left_attr_comp_index].type)
        {
            case TypeInt:
                if (*((int*)right_comp_data) == *((int*)left_comp_data))
                    equal = true;
                break;
            case TypeReal:
                if (*((float*)right_comp_data) == *((float*)left_comp_data))
                    equal = true;
                break;
            case TypeVarChar:
                uint32_t lVarcharSize;
                uint32_t rVarcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&lVarcharSize, left_comp_data, VARCHAR_LENGTH_SIZE);
                memcpy(&rVarcharSize, right_comp_data, VARCHAR_LENGTH_SIZE);

                char* left_string = (char*)malloc(lVarcharSize + 1);
                char* right_string = (char*)malloc(rVarcharSize + 1);

                memcpy(left_string, (char*)left_comp_data + VARCHAR_LENGTH_SIZE, lVarcharSize);
                memcpy(right_string, (char*)right_comp_data + VARCHAR_LENGTH_SIZE, rVarcharSize);

                left_string[lVarcharSize] = '\0';
                right_string[rVarcharSize] = '\0';

                if (strcmp(left_string, right_string)) {
                    equal = true;
                }

                free(left_string);
                free(right_string);
                // right_comp_data = malloc(VARCHAR_LENGTH_SIZE + varcharSize);
                // memcpy(right_comp_data, &varcharSize, VARCHAR_LENGTH_SIZE);
                // memcpy(right_comp_data, (char*)inner_page_data + right_comp_attr_offset + VARCHAR_LENGTH_SIZE, varcharSize);
                break;
        }
      
        if (!equal) {
            free(right_comp_data);
            free(left_comp_data);
            continue;
        }

        // if comparison is true
        // need to get the length of the left side, move it into the data param
        // then need to set offset, and then move the right side data in after 
        // that offset, but only the length of the data otherwise we might write
        // past the buffer of *data. We should add the lengths to ensure <4096
        unsigned inner_size = getRecordSize(right->attrs, inner_page_data);
        vector<Attribute> left_attrs;
        left->getAttributes(left_attrs);
        unsigned outer_size = getRecordSize(left_attrs, outer_page_data);
        if ((inner_size + outer_size) >= 4096)
            return JOIN_RSLT_TOO_BIG;

        memcpy(data, outer_page_data, outer_size);
        memcpy((char*)data + outer_size, inner_page_data, inner_size);
        free(right_comp_data);
        free(left_comp_data);
    }

    if (inner_rc == QE_EOF) {
        right->setIterator(NULL, NULL, true, true);
        outer_rc = left->getNextTuple(outer_page_data); // can either do this OR can set newLeft = true
        if (outer_rc == QE_EOF) {
            free(inner_page_data);
            return outer_rc;
        }

        if (outer_rc) {
            free(inner_page_data);
            return outer_rc;
        }
    }

    free(inner_page_data);
    return getNextTuple(data);
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs = total_attrs;
}

int INLJoin::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

unsigned INLJoin::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;

        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
                break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
                break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
                break;
        }
    }

    return offset;
}

int INLJoin::getAttributeOffset(void* data, bool left) {
    int offset = 0;
    int attr_index;
    char* nullIndicator = NULL;
    if(left) {
        attr_index = left_attr_comp_index;
        int nullIndicatorSize = getNullIndicatorSize(left_attrs.size());
        offset += nullIndicatorSize;
        nullIndicator = (char*)calloc(nullIndicatorSize, 1);
        memcpy(nullIndicator, data, nullIndicatorSize);
    }
    else {
        attr_index = right_attr_comp_index;
        int nullIndicatorSize = getNullIndicatorSize(right_attrs.size());
        offset += nullIndicatorSize;
        nullIndicator = (char*)calloc(nullIndicatorSize, 1);
        memcpy(nullIndicator, data, nullIndicatorSize);
    }

    if (left) {
        for(int i = 0; i < attr_index; i += 1) {
            if (fieldIsNull(nullIndicator, i))
                continue;
            switch (left_attrs[i].type)
            {
                case TypeInt:
                    offset += INT_SIZE;
                    break;
                case TypeReal:
                    offset += REAL_SIZE;
                    break;
                case TypeVarChar:
                    uint32_t varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                    // size += varcharSize;
                    offset += varcharSize + VARCHAR_LENGTH_SIZE;
                    break;
            }
        }
    }
    else {
        for(int i = 0; i < attr_index; i += 1) {
            if (fieldIsNull(nullIndicator, i))
                continue;
            switch (right_attrs[i].type)
            {
                case TypeInt:
                    offset += INT_SIZE;
                    break;
                case TypeReal:
                    offset += REAL_SIZE;
                    break;
                case TypeVarChar:
                    uint32_t varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                    // size += varcharSize;
                    offset += varcharSize + VARCHAR_LENGTH_SIZE;
                    break;
            }
        }
    }
  
    return offset;
}

bool INLJoin::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

bool Filter::checkScanCondition(int recordInt, CompOp compOp, const void *value)
{
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);
    /* cerr << "int scan cond found a value of: " << intValue << endl; */

    switch (compOp)
    {
        case EQ_OP: return recordInt == intValue;
        case LT_OP: return recordInt < intValue;
        case GT_OP: return recordInt > intValue;
        case LE_OP: return recordInt <= intValue;
        case GE_OP: return recordInt >= intValue;
        case NE_OP: return recordInt != intValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::checkScanCondition(float recordReal, CompOp compOp, const void *value)
{
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordReal == realValue;
        case LT_OP: return recordReal < realValue;
        case GT_OP: return recordReal > realValue;
        case LE_OP: return recordReal <= realValue;
        case GE_OP: return recordReal >= realValue;
        case NE_OP: return recordReal != realValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::checkScanCondition(char *recordString, CompOp compOp, const void *value)
{
    if (compOp == NO_OP)
        return true;

    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    int cmp = strcmp(recordString, valueStr);
    switch (compOp)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        // Should never happen
        default: return false;
    }
}

// // INLJoin versions
// bool INLJoin::checkScanCondition(int recordInt, CompOp compOp, const void *value)
// {
//     int32_t intValue;
//     memcpy (&intValue, value, INT_SIZE);

//     switch (compOp)
//     {
//         case EQ_OP: return recordInt == intValue;
//         case LT_OP: return recordInt < intValue;
//         case GT_OP: return recordInt > intValue;
//         case LE_OP: return recordInt <= intValue;
//         case GE_OP: return recordInt >= intValue;
//         case NE_OP: return recordInt != intValue;
//         case NO_OP: return true;
//         // Should never happen
//         default: return false;
//     }
// }

// bool INLJoin::checkScanCondition(float recordReal, CompOp compOp, const void *value)
// {
//     float realValue;
//     memcpy (&realValue, value, REAL_SIZE);

//     switch (compOp)
//     {
//         case EQ_OP: return recordReal == realValue;
//         case LT_OP: return recordReal < realValue;
//         case GT_OP: return recordReal > realValue;
//         case LE_OP: return recordReal <= realValue;
//         case GE_OP: return recordReal >= realValue;
//         case NE_OP: return recordReal != realValue;
//         case NO_OP: return true;
//         // Should never happen
//         default: return false;
//     }
// }

// bool INLJoin::checkScanCondition(char *recordString, CompOp compOp, const void *value)
// {
//     if (compOp == NO_OP)
//         return true;

//     int32_t valueSize;
//     memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
//     char valueStr[valueSize + 1];
//     valueStr[valueSize] = '\0';
//     memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

//     int cmp = strcmp(recordString, valueStr);
//     switch (compOp)
//     {
//         case EQ_OP: return cmp == 0;
//         case LT_OP: return cmp <  0;
//         case GT_OP: return cmp >  0;
//         case LE_OP: return cmp <= 0;
//         case GE_OP: return cmp >= 0;
//         case NE_OP: return cmp != 0;
//         // Should never happen
//         default: return false;
//     }
// }
