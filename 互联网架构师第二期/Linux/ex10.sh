# /bin/sh
# select var in [params] do .. done
select var in "Java" "C++" "PHP" "Linux" "Python" "Ruby" "C#"
do
break
done
echo "you selected $var"