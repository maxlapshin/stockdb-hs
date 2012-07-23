all: stockdb



stockdb: stockdb.hs
	ghc -o stockdb stockdb.hs


run: stockdb
	./stockdb out.stock
