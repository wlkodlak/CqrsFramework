Požadavek: Definice nářadí
	Jako správce skladu chci přidat nářadí do seznamu

Scénář: Definice pomocí všech tří součástí
	Pokud je seznam nářadí prázdný
	Když definuji nové nářadí pro seznam s těmito hodnotami:
		| Výkres | 3-26-006     |
		| Rozměr | TCMT1102xx   |
		| Druh   | Držák plátku |
	Pak seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |

Scénář: Rozměr a druh není nutné zadávat
	Pokud je seznam nářadí prázdný
	Když definuji nové nářadí pro seznam s těmito hodnotami:
		| Výkres | 3-26-006     |
		| Rozměr |              |
		| Druh   |              |
	Pak seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 |               |              |

Scénář: Výkres je nutné zadat
	Pokud je seznam nářadí prázdný
	Když definuji nové nářadí pro seznam s těmito hodnotami:
		| Výkres |              |
		| Rozměr | TCMT1102xx   |
		| Druh   | Držák plátku |
	Pak dostanu varování, že není možné definovat nářadí bez výkresu

Scénář: Kombinace výkresu a rozměru musí být unikátní
	Pokud seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
	Když definuji nové nářadí pro seznam s těmito hodnotami:
		| Výkres | 3-26-006     |
		| Rozměr | TCMT1102xx   |
		| Druh   |              |
	Pak dostanu varování, že není možné definovat nářadí, které už v seznamu existuje

Scénář: Celá kombinace výkresu a rozměru musí být unikátní, rozdíl i v jedné z hodnot zajistí unikátnost
	Pokud seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
	Když definuji nové nářadí pro seznam s těmito hodnotami:
		| Výkres | 3-26-006     |
		| Rozměr |              |
		| Druh   |              |
	Pak seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 3-26-006 |               |              |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |

