# Generate test data on the large server
# You will need to have the tpcds dsdgen program built in the current directory

gen() {
  CPU=$1
  SCALE=$2
  SEED=$3
  seq 1 $CPU \
    | xargs -t -P$CPU -I__ \
        ./dsdgen \
          -SCALE $SCALE \
          -DELIMITER \| \
          -PARALLEL $CPU \
          -CHILD __ \
          -TERMINATE N \
          -RNGSEED $SEED \
          -DIR ./tpcds_1000/
}

gen 16 100 42
gen 16 1000 42
