# hotstuff 2chain
2chain hotstuff implementation in Golang. The consensus module is largely converted from asonnino/hotstuff. The major difference is the use of LibP2P instead of Tokio and BLST instead of Dalek. Consensus and the mempool are fully functional, but can be cleaned up more.

AI was used to convert consensus/timer.go (I will probably remove this soon, it's useless). The ReadJSON and WriteJSON functions in node/config.go are also generated if I recall correctly, I'm not really sure why.
