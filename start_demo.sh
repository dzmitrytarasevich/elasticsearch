docker-compose up -d
echo "Sleeping for 20seconds for grafana, elastic and kibana to start"
sleep 20
nohup python demo_generator.py  & 
