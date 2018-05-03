directories="analytics_platform evaluation_platform tagging_platform tests"

# checks for the whole directories
for directory in $directories
do
    grep -r -n "TODO: " $directory
done
