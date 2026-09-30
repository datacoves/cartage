Output docs/cartage-demo.gif
Require uv
Set Shell "zsh"
Set FontSize 14
Set Width 1100
Set Height 680
Set TypingSpeed 25ms
Set Theme "Catppuccin Mocha"
Set LoopOffset 4%

Hide
Type `CARTAGE_ROOT="$PWD"; DEMO_DIR="$(mktemp -d /tmp/cartage-demo.XXXXXX)"; trap 'rm -rf "$DEMO_DIR"' EXIT; uv run cartage init "$DEMO_DIR" >/dev/null && source "$CARTAGE_ROOT/.venv/bin/activate" && export PATH="$CARTAGE_ROOT/.venv/bin:$PATH" && hash -r && cd "$DEMO_DIR" && clear`
Enter
Show
Sleep 700ms

Type "Let's see what Cartage can do..."
Sleep 1s
Backspace 32
Type "cartage --help"
Enter
Wait+Line />$/
Sleep 3s

Type "First, let's validate the demo project."
Sleep 1s
Backspace 39
Type "cartage validate"
Enter
Wait+Line />$/
Sleep 2s

Type "Watch the UOM conversion: KGS to KG."
Sleep 1s
Backspace 36
Type "cartage plan materials -n 3"
Enter
Wait+Line />$/
ScrollUp 30
Sleep 1s
ScrollDown@100ms 30
Sleep 2s

Type "Now generate the production Airflow DAG."
Sleep 1s
Backspace 40
Type "cartage generate"
Enter
Wait+Line />$/
Sleep 2s

Type "Let's read the generated Airflow DAG."
Sleep 1s
Backspace 37
Type "cat dags/materials_to_sap.py"
Enter
Wait+Line />$/
Sleep 4s

Type "Finally, run the sample materials pipeline."
Sleep 1s
Backspace 43
Type "cartage run materials"
Enter
Wait+Line />$/
Sleep 4s
