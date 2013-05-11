Požadavek: Příjem ze skladu
	Jako skladník chci přijímat nové nářadí z hlavního skladu na výdejnu

Kontext:
	Pokud seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
	A nebyly provedeny žádné operace

Scénář: Příjem nového nářadí s vyplněnými všemi hodnotami
	Když přijímám nové nářadí na výdejnu ze skladu s hodnotami:
		| Výkres    | 3-26-006    |
		| Rozměr    | TCMT1102xx  |
		| Počet     | 5           |
		| Cena      | 3480.00     |
		| Dodavatel | VNP s.r.o.  |
	Pak je přijato nové nářadí na výdejnu ze skladu s hodnotami:
		| Výkres    | 3-26-006    |
		| Rozměr    | TCMT1102xx  |
		| Počet     | 5           |
		| Cena      | 3480.00     |
		| Dodavatel | VNP s.r.o.  |

Scénář: Dodavatele není nutné vyplňovat
	Když přijímám nové nářadí na výdejnu ze skladu s hodnotami:
		| Výkres    | 3-26-006    |
		| Rozměr    | TCMT1102xx  |
		| Počet     | 5           |
		| Cena      | 3480.00     |
		| Dodavatel |             |
	Pak je přijato nové nářadí na výdejnu ze skladu s hodnotami:
		| Výkres    | 3-26-006    |
		| Rozměr    | TCMT1102xx  |
		| Počet     | 5           |
		| Cena      | 3480.00     |
		| Dodavatel |             |

Scénář: Cena musí být nezáporná
	Když přijímám nové nářadí na výdejnu ze skladu s hodnotami:
		| Cena      | -480.00     |
	Pak dostanu varování, že cena nesmí být záporná

Scénář: Množství musí být kladné
	Když přijímám nové nářadí na výdejnu ze skladu s hodnotami:
		| Počet     | 0           |
	Pak dostanu varování, že množství musí být kladné

Scénář: Není možné přijímat nářadí, které není v seznamu používaného nářadí:	
	Když přijímám nové nářadí na výdejnu ze skladu s hodnotami:
		| Výkres    | 3-26-018    |
		| Rozměr    | WAISSER     |
	Pak dostanu varování, že není možné přijímat nářadí, které není v seznamu
