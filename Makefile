all: stockdb



stockdb: stockdb.hs
	ghc -O2 -fllvm -o stockdb stockdb.hs -Wall -Werror -rtsopts -hide-package AC-Vector-Fancy


run: stockdb
	\time -v ./stockdb out.stock +RTS -A1000000
