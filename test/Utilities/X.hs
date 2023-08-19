module Utilities.X where 

-- sudo iptables -A OUTPUT -p tcp --destination-port %d -j DROP
-- sudo iptables -A INPUT -p tcp --destination-port %d -j DROP

-- sudo iptables -D OUTPUT -p tcp --destination-port %d -j DROP
-- sudo iptables -D INPUT -p tcp --destination-port %d -j DROP

--sudo tc qdisc add dev %s root netem delay %dms %dms distribution normal
--sudo tc qdisc change dev %s root netem delay %dms %dms distribution normal
--sudo tc qdisc del dev %s root netem
 
