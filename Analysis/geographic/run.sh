Schemes="agriculture humanDevelopment environment industrialization tourism_culture health_hygiene"
add="_schemes"
for val in $Schemes; do
    	echo $val$add
	python3 loc.py $val$add
done

