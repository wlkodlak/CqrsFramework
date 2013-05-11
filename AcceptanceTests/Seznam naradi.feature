Požadavek: Seznam nářadí
	Jako správce skladu	chci mít k dispozici seznam používaného nářadí

Scénář: Každé nářadí je určeno pomocí trojice: výkres, rozměr a druh
	Pokud seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
	Pak seznam používaného nářadí obsahuje přesně tuto tabulku:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |

Scénář: Seznam používaného nářadí je seřazen podle výkresu
	Pokud seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
	Pak je nářadí s výkresem 3-26-006 zobrazeno v seznamu používaného nářadí dříve než nářadí s výkresem 4-41-073.

Scénář: Seznam používaného nářadí neobsahuje smazané nářadí
	Pokud seznam nářadí tvoří:
		| Výkres   | Rozměr        | Druh         |
		| 3-26-006 | TCMT1102xx    | Držák plátku |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
	A nářadí s výkresem 3-26-006 bylo smazáno
	Pak seznam používaného nářadí obsahuje přesně tuto tabulku:
		| Výkres   | Rozměr        | Druh         |
		| 4-41-073 | průměr 25/5,8 | Opěrka hlavy |
