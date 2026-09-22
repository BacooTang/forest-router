module.exports = {
  apps: [{
    name: 'forest-router',
    script: './target/release/forest-router',
    interpreter: 'none',
    args: '--port 8119', // 部署时改为 --port 80
    instances: 1,
    kill_timeout: 15000,
    max_memory_restart: '256M',
    env: { FOREST_ROUTER_HOME: './data' }
  }]
};
