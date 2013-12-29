all: stockdb



stockdb: stockdb.hs
	ghc -O2 -fllvm -o stockdb stockdb.hs -hide-package AC-Vector-Fancy


run: stockdb
	\time -v ./stockdb out.stock
