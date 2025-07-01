source ~/.bashrc
ln -s /install/node_modules node_modules
if [ -f "./windows" ]; then
  echo "devStart"
  yarn devStart
else
  echo "start"
  yarn start
fi

