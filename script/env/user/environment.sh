# ============================================================================
# Titolo: environment.sh
# Descrizione: Contiene la definizione delle variabili di ambiente per una shell bash
# Autore: Alfredo Milani (alfredo.milani.94@gmail.com)
# Data: Mon Nov 19 06:01:50 CET 2020
# Licenza: MIT License
# Versione: 1.0.0
# Note: --/--
# Versione bash: 4.4.19(1)-release
# ============================================================================


# Update PATH variable adding custom scripts
export PATH="${PATH}:/usr/local/sbin:/usr/local/etc/script"

# Default ramdisk path
export RAMDISK='/Volumes/Ramdisk'
# Default ramdisk backup path
export RAMDISK_BU="${HOME}/Ramdisk"
# Trash path on ramdisk
export TRASH_R="${RAMDISK}/Trash"
# Projects' path
export PROJ="/Volumes/Data/Projects"
# Datas' path
export DATA="/Volumes/Data/Data"

# Default visual editor
export VISUAL='/Applications/"Sublime Text.app"/Contents/MacOS/"Sublime Text"'
# Default line editor
export EDITOR='/usr/bin/vim'
# Default terminal application
export TERMINAL='/Applications/iTerm.app/Contents/MacOS/iTerm2'
# Default media player application
export PLAYER='/Applications/VLC.app/Contents/MacOS/VLC'

# Algoritmo di default per cifrare/decifrare file utilizzando GPG
export CIPHER_ALGO='aes128'
# Path del file audio utilizzato per le funzioni di timer e sveglia
export TIMEOUT_SOUND='/Volumes/Data/Configurations/Utils/Timer allarm/military-trumpet.mp3'

# Tool per notificare il completamento di task
export NOTIFIER='/usr/local/opt/terminal-notifier-2.0.0/terminal-notifier.app/Contents/MacOS/terminal-notifier'
