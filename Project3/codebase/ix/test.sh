make clean
make
valgrind ./ixtest_01
valgrind ./ixtest_02
valgrind --leak-check=full ./ixtest_03
valgrind --leak-check=full ./ixtest_04
valgrind ./ixtest_05
# valgrind ./ixtest_06
# valgrind ./ixtest_07
# valgrind ./ixtest_08
# valgrind ./ixtest_09
# valgrind ./ixtest_10
# valgrind ./ixtest_11
# valgrind ./ixtest_12
# valgrind ./ixtest_13
# valgrind ./ixtest_14
# valgrind ./ixtest_15
