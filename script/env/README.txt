The bash_mod directory enables user to load custom bash shell environment dinamically.

File ./load.sh manages to load base components contained in ./core directory.
Directory ./core contains:
    - ./environment.sh: contains definition about base environment variable;
    - ./alias.sh: contains base aliases definition;
    - ./function.sh: contains base function definition.

Every module directory will be managed after core components loading.
Scripts with ".off" extension will not be loaded.
