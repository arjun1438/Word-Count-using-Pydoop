step1()
{
   
   echo "you chose to run Step 1 i.e. count frequency of word in all docs"
   echo -n "Enter number of reducers > "
   read reducers_count
   hadoop fs -rm -R /bigd21/Step1Output
	 pydoop submit --num-reducers $reducers_count --inputformat=org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat --upload-file-to-cache word_count_per_bookv3.py word_count_per_bookv3 /cosc6339_s17/books-longlist-directory.txt  /bigd21/Step1Output
   hadoop fs -getmerge /bigd21/Step1Output/* step1_output.txt

}

step2()
{
   echo "you chose to run Step 2 to find the no of docs in which word is present"
   echo -n "Enter number of reducers > "
   read reducers_count
   hadoop fs -rm -R /bigd21/Step2Input
   hadoop fs -mkdir -p /bigd21/Step2Input
   hadoop fs -put step1_output.txt /bigd21/Step2Input
   hadoop fs -rm /bigd21/step2_input.txt
   echo "/bigd21/Step2Input/step1_output.txt" > "./step2_input.txt"
   hadoop fs -put step2_input.txt /bigd21
   hadoop fs -rm -R /bigd21/Step2Output
   pydoop submit --num-reducers $reducers_count --inputformat=org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat --upload-file-to-cache word_in_no_of_docsv4.py word_in_no_of_docsv4 /bigd21/step2_input.txt  /bigd21/Step2Output
   hadoop fs -getmerge /bigd21/Step2Output/* step2_output.txt
}

runAll()
{
    step1
    step2
}   


PS3='Please enter your choice: '
options=("Step 1" "Step 2" "runAll" "Quit")
select opt in "${options[@]}"
do
    case $opt in
        "Step 1")
            step1
            ;;
        "Step 2")
            step2
            ;;
        "runAll")
            runAll
            ;;
        "Quit")
            break
            ;;
        *) echo invalid option;;
    esac
done
