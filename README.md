# hotstuff 2chain
2chain hotstuff implementation in Golang. The consensus and mempool modules is largely converted from asonnino/hotstuff. The major difference is the use of LibP2P instead of Tokio and BLST instead of Dalek. Consensus and the mempool are fully functional, but can be cleaned up more. Particularly, there are way too many debug print statements.

AI was used to convert consensus/timer.go (I will probably remove this soon, it's useless). The ReadJSON and WriteJSON functions in node/config.go are also generated if I recall correctly. Everything else is by hand.
