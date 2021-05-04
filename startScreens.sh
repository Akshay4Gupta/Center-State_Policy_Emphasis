Schemes="agri dev env indus lfstyl"
# add=_oc
for val in $Schemes; do
    screen -xS $val$add
done
