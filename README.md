# hotstuff 2chain
2chain hotstuff implementation in Golang. The consensus module is largely converted from asonnino/hotstuff. The major difference is the use of LibP2P instead of Tokio and BLST instead of Dalek. Consensus itself is fully working, with only some polish needed. The mempool/payload section of the program is still in debugging stage.

AI was used to convert consensus/timer.go, but everything else is by hand.
