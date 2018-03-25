
## Goroutines 

- `timer`:  will send `times out` message to `main`.

- `RPC listener`: listen to `AppendEntries` and `RequestVote` rpc, and will send `new term` or `new leader` message to main.

- `candidateMode`: vote for itself, and issues RequestVote RPCs in parallel to each of the other servers. If this goroutine receives votes from majority of servers, it will send `winElection` message to main, and then the main goroutine will switch its state.    

- `followerMode`: this goroutine will do nothing.

- `leaderMode`: keep sending `heartbeat` messages to other servers. 

- `main`: state switch due to different event:
    - if the event is `times out`: 
        - if currrent state is `FOLLOWER`, then switch state to `CANDIDATE`, `currentTerm += 1`, and then start the `candidateMode` goroutine, suspend.
        - if current state is `CANDIDATE`, then `currentTerm += 1`, suspend the older `candidateMode` goroutine, and restart a new one.
        - if current state is `LEADER`, then ignore this event.

    - if the event is `winElection`: then switch state to `LEADER`, and then suspend the candidateMode goroutine, start the `leaderMode` goroutine.

    - if the event is `new term`: then switch state to `FOLLOWER`, and then start the `followerMode` goroutine.
        
    - if the event is `new leader`: then switch state to `FOLLOWER`, and then start the `followerMode` goroutine.

