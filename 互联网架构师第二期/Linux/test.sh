my_array=(A,B,C,D)
#my_array[0]=A
#my_array[1]=B
#my_array[2]=C
#my_array[3]=D
echo "数组的元素为:${my_array[*]}"
echo "数组的元素为:${my_array[@]}"
echo "数组的元素个数为:${#my_array[*]}"
echo "数组的元素个数为:${#my_array[@]}"
num=${#my_array[*]}
echo "数组的长度为$num"
for((i=0;i<num;i++))
{
        echo "索引为：$i 的元素为: ${my_array[i]}";
}
echo "获取某个单元的长度 ${#my_array[1]}"
for var in ${my_array[*]}
do
        echo $var
done
str="hello"
echo "len = ${#str}"