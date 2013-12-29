all: stockdb



stockdb: stockdb.hs
	ghc -O2 -fllvm -o stockdb stockdb.hs


run: stockdb
	./stockdb out.stock
