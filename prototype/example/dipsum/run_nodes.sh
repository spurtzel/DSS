mkdir -p mem_consumption

for j in {0..19}
do
    nohup /usr/bin/time -v -o "mem_consumption/${j}.mem" python3 sender.py $j > "output/output_${j}.txt" 2>&1 &
done
