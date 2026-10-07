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
Type `CARTAGE_ROOT="$PWD"; DEMO_DIR="$(mktemp -d /tmp/cartage-demo.XXXXXX)"; trap 'rm -rf "$DEMO_DIR"' EXIT; cp -R "$CARTAGE_ROOT/examples/sap" "$DEMO_DIR/sap-example" && source "$CARTAGE_ROOT/.venv/bin/activate" && export PATH="$CARTAGE_ROOT/.venv/bin:$PATH" && hash -r && cd "$DEMO_DIR" && clear`
Enter
Show
Sleep 1s
Type "Let's see what Cartage can do..."
Sleep 2.5s
Backspace 32
Type "cartage --help"
Sleep 800ms
Enter
Wait+Line />$/
Sleep 7s

Type "Create a project from the SAP example's answers..."
Sleep 2.5s
Backspace 50
Type "cartage init demo --answers sap-example/answers.yaml --yes"
Sleep 800ms
Enter
Wait+Line />$/
Sleep 6s

Type "Check every pipeline, connection and transform."
Sleep 2.5s
Backspace 47
Type "cd demo && cartage validate"
Sleep 800ms
Enter
Wait+Line />$/
Sleep 5s

Type "Preview the transforms: watch the UOM conversion, KGS to KG."
Sleep 3s
Backspace 60
Type "cartage plan materials -n 3"
Sleep 800ms
Enter
Wait+Line />$/
ScrollUp 30
Sleep 3s
ScrollDown@250ms 30
Sleep 5s

Type "Now generate the production Airflow DAG."
Sleep 2.5s
Backspace 40
Type "cartage generate && cat dags/materials.py"
Sleep 800ms
Enter
Wait+Line />$/
Sleep 8s

Type "Finally, run the sample materials pipeline."
Sleep 2.5s
Backspace 43
Type "cartage run materials"
Sleep 800ms
Enter
Wait+Line />$/
Sleep 8s
