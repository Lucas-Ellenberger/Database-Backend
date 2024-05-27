make clean
make
valgrind ./ixtest_01
valgrind ./ixtest_02
valgrind ./ixtest_03
valgrind ./ixtest_04
valgrind ./ixtest_05
valgrind ./ixtest_06
valgrind ./ixtest_07
valgrind ./ixtest_08
valgrind ./ixtest_09
valgrind ./ixtest_10
valgrind ./ixtest_11 > out.txt
# valgrind ./ixtest_12
# valgrind ./ixtest_13
# valgrind ./ixtest_14
# valgrind ./ixtest_15
