Požadavek: Výdej do výroby
	Jako skladník chci vydávat nářadí na pracoviště výroby

Kontext:
	Pokud seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
	A bylo přijato nové nářadí na výdejnu ze skladu s hodnotami: 
		| Výkres    | 3-26-006    |
		| Rozměr    | TCMT1102xx  |
		| Počet     | 5           |
		| Cena      | 3480.00     |
		| Dodavatel | VNP s.r.o.  |

Scénář: Výdej na pracoviště výroby
	Když vydávám nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 3           |
		| Pracoviště | 11045330    |
	Pak je vydáno nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 3           |
		| Pracoviště | 11045330    |
	A na výdejně zbývají pro výdej na pracovitě 2 kusy nářadí s výkresem 3-26-006

Scénář: Stejné nářadí je možné vydávat na různá pracovitě, pokud je ještě co vydávat
	Pak bylo vydáno nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 3           |
		| Pracoviště | 11045330    |
	Když vydávám nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 2           |
		| Pracoviště | 54871320    |
	Pak je vydáno nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 2           |
		| Pracoviště | 54871320    |
	A na výdejně zbývá pro výdej na pracovitě 0 kusů nářadí s výkresem 3-26-006

Scénář: Nelze vydávat více kusů, než je k dispozici
	Pak bylo vydáno nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 3           |
		| Pracoviště | 11045330    |
	Když vydávám nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 4           |
		| Pracoviště | 54871320    |
	Pak dostanu varování, že nelze vydávat více kusů, než je k dispozici

Scénář: Pracoviště je nutné zadávat
	Když vydávám nářadí na pracoviště výroby s hodnotami:
		| Výkres     | 3-26-006    |
		| Rozměr     | TCMT1102xx  |
		| Počet      | 4           |
		| Pracoviště |             |
	Pak dostanu varování, že je nutné specifikovat pracoviště, kam se nářadí vydává

