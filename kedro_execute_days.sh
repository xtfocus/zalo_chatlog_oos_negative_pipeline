#!/bin/bash

# Check if dates.txt exists
if [ ! -f dates.txt ]; then
    echo "Error: dates.txt not found!"
    exit 1
fi

mkdir kedro_output

# Loop through each line in dates.txt
while IFS= read -r date; do
    export DAY_REQUEST="$date"
    echo "DAY_REQUEST=\"$DAY_REQUEST\" has been set"

    output_dir=kedro_output/output_${date}
    mkdir ${output_dir}
    kedro run
    mv data/08_model_output/* ${output_dir}
done < dates.txt

