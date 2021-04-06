module.exports = {
    apps : [  {
      name: 'balancer-api.swaps',
      script: 'index.js',
      watch: ["."],
      // Delay between restart
      watch_delay: 10000,
      ignore_watch : [".git", "dist", "node_modules", "logs", "api.tar.gz"],
    }]
  };