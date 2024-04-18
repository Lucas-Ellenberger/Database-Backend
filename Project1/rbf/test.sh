make clean
make
./rbftest1
./rbftest2
./rbftest3
./rbftest4
./rbftest5
./rbftest6
./rbftest7
valgrind --track-origins=yes ./rbftest8
valgrind --track-origins=yes ./rbftest8b
# ./rbftest9
# ./rbftest10
# ./rbftest11
# valgrind ./rbftest12
