# Ręczny test statusów i wznowienia Pi

Uruchamiaj kolejno, w nowej rozmowie na każdym kanale. Test zajmie około
9–10 minut. Pozostaw rozmowę otwartą; nie pytaj o postęp przed wynikiem.
To celowo ograniczony test Pi, a nie zmiana zwykłego routingu abonamenty → Pi.

Oczekuj około trzech statusów w trakcie pracy (pierwszy po co najmniej
90 sekundach, następne co co najmniej 180 sekund) oraz zakończenia i jednej
automatycznej odpowiedzi Hermesa. Rzeczywiste czasy zależą od watchdog tick,
narzędzi i inference. Kolejne krótkie kroki zapewniają zmiany obserwowanego
postępu; pojedynczy długi sleep nie daje takiego dowodu.

- Telegram: statusy w bieżącym czacie/temacie.
- Desktop/TUI: natywne powiadomienia; w Desktop jeden toast na zadanie,
  zastępowany następnym i widoczny przez 20 sekund. To nie powiadomienie macOS.
  Po aktualizacji backendu i pliku `desktop/plugin.js` rozwiń **Pokaż przebieg**:
  mają pojawiać się komendy, godziny i kolejne linie wyniku przed zakończeniem
  narzędzia. Przewiń wstecz: dopisywanie nie może przestawić widoku. Przycisk
  **Do najnowszych** ma wznowić śledzenie. Zwiń kartę i wyślij inną wiadomość
  Hermesowi; statusy mają nadal działać. Przełącz rozmowę/profil: stary dziennik
  nie może zostać pokazany w nowej rozmowie. Starsze zadania bez dziennika
  zachowują skrócony podgląd wiadomości i narzędzi.
- Klasyczny interaktywny CLI: aktualizowany natywny panel subagentów przy polu
  wpisywania. `Ctrl+T` / `F6` otwiera listę, `Enter` podgląd strumienia,
  `Esc` wraca, `F7` zwija panel. Wpisz szkic wiadomości, otwórz i zamknij
  podgląd: szkic ma pozostać. Podczas pracy Pi możesz wysłać Hermesowi inne
  polecenie; wynik Pi ma poczekać, aż ta tura się zakończy. Statusy nie
  uruchamiają modelu ani nie powtarzają się w historii. Starszy Hermes bez
  natywnego panelu nadal otrzymuje statusy tekstowe nad polem wpisywania.
  Otwórz nowy proces CLI po aktualizacji wtyczki, zanim zlecisz zadanie.
  W rozwiniętym podglądzie sprawdź godzinę i komendę przy starcie narzędzia,
  nowe linie wyniku przed zakończeniem komendy oraz czas i wynik po zakończeniu.
  Dziennik ma rosnąć; powtarzane częściowe wyniki RPC nie mogą dublować linii.
  Testuj komendą wypisującą tekst co kilka sekund, np. pętlą `printf` i `sleep`.
  Samo `sleep`, które wypisuje wynik dopiero na końcu, nie testuje strumieniowania.
  Zostaw podgląd otwarty także przez co najmniej 10 sekund po zakończeniu Pi.
  Następnie zamknij go przez `Esc` → `Esc` i wyślij `Ile wynosi 5 × 5?`.
  Końcowe powiadomienie i odpowiedź mają być widoczne w rozmowie, a zakończony
  Pi ma zniknąć z panelu. Nie powinno być wielokrotnych prób drukowania nad
  otwartym podglądem. Aby sprawdzić cichy start, pomiń w poniższym prompcie
  polecenie wypisania `task_id` — jawne żądanie identyfikatora nadal wygrywa.

## Prompt do wklejenia

```text
Wykonaj kontrolowany test Pi Managera w tej rozmowie. Wyraźnie zezwalam
na jednorazowe użycie Pi/Windows NInfer oraz jego statusy w bieżącym kanale.
Nie zmieniaj normalnego routingu, konfiguracji ani częstotliwości powiadomień.

1. Utwórz osobny katalog tymczasowy i uruchom dokładnie jedno pi_task,
   przekazując jego bezwzględną ścieżkę jako cwd. Zachowaj task_id.
   Nie ustawiaj verifier_argv: test nie zmienia plików. Możesz ustawić
   emergency_cap_seconds=1200 jako jawny limit awaryjny tego testu.

2. Przekaż workerowi poniższe instrukcje po angielsku:
   "This is an explicitly authorized notification smoke test. Execute ten
   sequential steps. For each N from 1 to 10, make a SEPARATE bash tool call
   running Python with only the standard library: sleep for 55 seconds, then
   print STEP N/10 and the first 12 hex characters of SHA256 of the UTF-8
   string pi-notify-N. Substitute the actual N. Do not combine steps into
   one long tool call and do not run them in parallel. After each tool result,
   briefly acknowledge that completed step before starting the next.
   Do not create or modify files, access the network, inspect credentials,
   modify infrastructure, send messages directly, or start another agent.
   If a step fails, report the real error; do not claim all steps completed.
   After ten successful steps, reply PI_NOTIFICATION_TEST_DONE 10/10,
   followed by the ten observed checksums."

3. Po uruchomieniu podaj task_id i zakończ swoją turę. Nie odpytuj pi_status,
   nie uruchamiaj pętli oczekiwania ani drugiego agenta. Statusy ma dostarczać
   sam Pi Manager; nie zastępuj ich ręcznymi wiadomościami.

4. Po automatycznym wznowieniu odczytaj pi_digest raz, sprawdź rezultat
   i odpowiedz: TEST ZAKOŃCZONY — albo uczciwie opisz niepowodzenie.
   Podaj task_id, liczbę wykonanych kroków i wynik zadania. Nie deklaruj,
   ile powiadomień zobaczyłem: ich widoczność sprawdzam sam.
```

Po próbie zanotuj kanał i wariant CLI/TUI, task_id, przybliżone czasy widocznych
statusów, ewentualne duplikaty oraz to, czy końcowa odpowiedź pojawiła się
samoczynnie. Przy podejrzeniu problemu dopiero wtedy użyj jednorazowego
`pi_status`/diagnostyki rejestru; nie maskuj awarii ręcznym wznowieniem testu.
