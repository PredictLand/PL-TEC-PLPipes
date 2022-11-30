import sys
import pathlib
sys.path.append(str(pathlib.Path(sys.argv[0])
                    .absolute()
                    .parent.parent.parent / "lib"))

from plpipes.runner import main
main()
