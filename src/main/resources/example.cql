CREATE KEYSPACE example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} ;

Create TABLE example.fruits(
	id text,
	fruit_list list<text>,
	PRIMARY KEY (id)
);

INSERT INTO example.fruits
  (id, fruit_list)
  VALUES ('1',['apple', 'banana']);

Create TABLE example.mix(
	id text,
	ls list<text>,
	mp map<text, text>,
	lsmp frozen <list<map<text, text>>>,
	PRIMARY KEY (id)
);
