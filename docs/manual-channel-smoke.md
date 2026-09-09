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
- Klasyczny interaktywny CLI: testuj automatyczne wznowienie. Obecny plugin
  nie ma pasywnego celu powiadomień dla sesji bez platform/chat. Brak statusów
  w tym wariancie jest istniejącym ograniczeniem, nie obietnicą tego promptu.

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
