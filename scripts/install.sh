PROJECT="office365-service"

echo "Instalando $PROJECT ..."

# criar repositório git
mkdir -p /var/repo/$PROJECT.git
cd /var/repo/$PROJECT.git || exit
git init --bare
cd hooks || exit
cat > /var/repo/$PROJECT.git/hooks/post-receive <<EOF
#!/bin/sh
WORK_TREE=/var/opt/$PROJECT
if [ -d \$WORK_TREE ]; then
  git --work-tree=\$WORK_TREE --git-dir=/var/repo/$PROJECT.git checkout -f
else
  git clone /var/repo/$PROJECT.git \$WORK_TREE
fi
EOF
chmod +x post-receive

if [ ! -e "$HOME"/.local/bin/uv ]; then
  echo "instalando uv ..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  # Check if $HOME/.local/bin is already in PATH
  if ! echo "$PATH" | grep -q "$HOME/.local/bin"; then
    echo "export PATH=\$PATH:\$HOME/.local/bin" >> ~/.bashrc
  fi
fi

echo "instalando dependencias ..."
npm install -g pm2
