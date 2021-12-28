sht:
	@echo " Compile sht_main ...";
	gcc -g -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/hash_file.c ./src/sht_file.c -lm -lbf -o ./build/runner -O2

ht:
	@echo " Compile ht_main ...";
	gcc -g -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/hash_file.c -lm -lbf -o ./build/runner -O2

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c -lbf -o ./build/runner -O2

ht2:
	@echo " Compile ht_secondMain ...";
	gcc -g -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_secondMain.c ./src/hash_file.c -lm -lbf -o ./build/runner -O2
