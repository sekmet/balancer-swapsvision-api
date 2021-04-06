const axios = require('axios')
const Pool = require('pg').Pool
const dotenv = require('dotenv')

dotenv.config()
const pool = new Pool({
  user: process.env.DB_USERNAME,
  host: process.env.DB_HOST,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
})


const getCoingeckoInfo = (request, response) => {
    let result = {}
    pool.query("SELECT * from api.coingecko WHERE symbol = $1", [request.params.symbol], (error, results) => {
        if (error) {
          result.Response = "error"  
          result.Message = error
          throw error
        }
    
        result.Data = results.rows
        result.Response = "success"
    
        response.status(200).json(result)
      }) 

}

const getTopTokensByPools = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 3000
  pool.query("select sum(liquidity) as total_liquidity, tokens_info as pair from api.pools group by pair order by total_liquidity DESC LIMIT $1", [_limit], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}

const getTopTokens = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 10
  let _skip = 0
  let _sortby = request.params.sortby ? request.params.sortby: 'total_balance'
  if (request.params.limit > 10){
  _skip = request.params.limit - 10
  _limit = 10
  }

  pool.query(`SELECT id, name, symbol, image, total_balance, (total_balance*market_price) AS liquidity, total_volume, price_usdc, price_dai, price_weth, market_price, price_change_percentage_24h AS percent24hs, price_change_24h AS change24hs from api.prices WHERE (price_usdc > 0 OR price_dai > 0 OR price_weth > 0) AND market_price > 0 ORDER BY ${_sortby} DESC LIMIT $1 OFFSET $2`, [_limit, _skip], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}

const getSwapsStats = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 100
  pool.query("SELECT count(id) AS total_transactions, SUM(amount) as total_amount, pair, AVG(price) AS avg_price, MAX(time) AS last_swap, time_bucket('1440 minute', time) AS tb FROM api.swaps GROUP BY pair,tb LIMIT $1", [_limit], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}


const getTopTokensRanking = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 1000
  let _skip = 0
  let _orderby = request.params.sortby ? request.params.sortby : 'liquidity'
  let _orderdirection = request.params.sortdirection ? request.params.sortdirection : 'DESC'
  if (request.params.limit > 10){
    _skip = request.params.limit - 10
    _limit = 10
  }  
  pool.query(`SELECT * from api.toptokens WHERE price > 0 ORDER BY ${_orderby} ${_orderdirection} LIMIT $1 OFFSET ${_skip}`, [_limit], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}

const getAllPrices = (request, response) => {
    let result = {}
    let _limit = request.params.limit ? request.params.limit: 100
    pool.query("SELECT * from api.prices WHERE (price_usdc > 0 OR price_dai > 0 OR price_weth > 0) AND market_price > 0 ORDER BY pools_marketcap DESC LIMIT $1", [_limit], (error, results) => {
      if (error) {
        result.Response = "error"  
        result.Message = error
        throw error
      }
  
      result.Data = results.rows
      result.Response = "success"
  
      response.status(200).json(result)
    })

}

const getAllTokens = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 1000
  pool.query("SELECT symbol from api.prices WHERE price_usdc > 0 OR price_dai > 0 OR price_weth > 0 LIMIT $1", [_limit], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}

const getAllPools = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 100
  let _orderby = request.params.orderby ? request.params.orderby : 'swaps_count'
  let _orderdirection = request.params.orderdirection ? request.params.orderdirection : 'DESC'
  pool.query(`SELECT * from api.pools ORDER BY ${_orderby} ${_orderdirection} LIMIT $1`, [_limit], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}


const getAllPairs = (request, response) => {
    let result = {}
    let _limit = request.params.limit ? request.params.limit: 100
    pool.query("SELECT pair from api.pairs LIMIT $1", [_limit], (error, results) => {
      if (error) {
        result.Response = "error"  
        result.Message = error
        throw error
      }
  
      result.Data = results.rows
      result.Response = "success"
  
      response.status(200).json(result)
    })

}

const getLastCandle = (request, response) => {
  let result = {}
  const { exchange, resolution, fsym, tsym, toTs, limit, orderby, orderdirection } = request.body
  const currentPair = `${fsym}-${tsym}`
  let _resolution = '60 minute'
  if (parseInt(resolution, 10) > 720) {
    _resolution = `1 day`
  } else {
    _resolution = `${parseInt(resolution, 10)} minute`
  }

  console.log(resolution, _resolution, fsym, tsym, toTs, limit, orderby, orderdirection)
  let _toTs = toTs ? toTs : Date.now()
  let _limit = 1
  let _orderby = orderby ? orderby : 'tb'
  let _orderdirection = orderdirection ? orderdirection : 'ASC'

  pool.query(`SELECT time_bucket('${_resolution}', time) as tb, first(price, time) AS open, last(price, time) AS close, MAX(price) AS high, MIN(price) AS low, SUM(amount) AS volume, price FROM api.swaps WHERE pair=$1 GROUP BY tb, pair, price ORDER BY ${_orderby} ${_orderdirection} LIMIT 1`, [currentPair], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}

const getHistory = (request, response) => {
  let result = {}
  const { exchange, resolution, fsym, tsym, toTs, limit, orderby, orderdirection } = request.body
  const currentPair = `${fsym}-${tsym}`
  let _resolution = '60 minute'
  if (resolution && (resolution.includes('D') || resolution === '1D' || resolution === 'D')) {
    _resolution = `1440 minute`
  } else if (parseInt(resolution, 10) > 720) {
    _resolution = `1440 minute`
  } else {
    _resolution = `${parseInt(resolution, 10)} minute`
  }

  console.log(resolution, _resolution, fsym, tsym, toTs, limit, orderby, orderdirection)
  let _toTs = toTs ? toTs : Date.now()
  let _limit = limit ? limit : '2000'
  let _orderby = orderby ? orderby : 'tb'
  let _orderdirection = orderdirection ? orderdirection : 'ASC'

  pool.query(`SELECT time_bucket('${_resolution}', time) as tb, first(price, time) AS open, last(price, time) AS close, MAX(price) AS high, MIN(price) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}


const getHistoryDay = (request, response) => {
  let result = {}
  const { exchange, fsym, tsym, toTs, limit, orderby, orderdirection } = request.body
  const currentPair = `${fsym}-${tsym}`
  let _limit = limit ? limit : '2000'
  let _orderby = orderby ? orderby : 'tb'
  let _orderdirection = orderdirection ? orderdirection : 'ASC'

  pool.query(`SELECT time_bucket('1 day', time) as tb, first(price, time) AS open, last(price, time) AS close, MAX(price) AS high, MIN(price) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
    if (error) {
      result.Response = "error"  
      result.Message = error
      throw error
    }

    result.Data = results.rows
    result.Response = "success"

    response.status(200).json(result)
  })

}

const getHistoryHour = (request, response) => {
    let result = {}
    const { exchange, fsym, tsym, toTs, limit, orderby, orderdirection } = request.body
    const currentPair = `${fsym}-${tsym}`
    let _limit = limit ? limit : '2000'
    let _orderby = orderby ? orderby : 'tb'
    let _orderdirection = orderdirection ? orderdirection : 'ASC'

    pool.query(`SELECT time_bucket('4 hour', time) as tb, first(price, time) AS open,last(price, time) AS close, MAX(price) AS high, MIN(price) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
    if (error) {
        result.Response = "error"  
        result.Message = error
        throw error
        }
    
        result.Data = results.rows
        result.Response = "success"
    
        response.status(200).json(result)
    })

}


const getHistoryMinute = (request, response) => {
    let result = {}
  const { exchange, fsym, tsym, toTs, limit, orderby, orderdirection } = request.body
  const currentPair = `${fsym}-${tsym}`
  let _limit = limit ? limit : '2000'
  let _orderby = orderby ? orderby : 'tb'
  let _orderdirection = orderdirection ? orderdirection : 'ASC'

    pool.query(`SELECT time_bucket('1 minute', time) as tb, first(price, time) AS open, last(price, time) AS close, MAX(price) AS high, MIN(price) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
        if (error) {
            result.Response = "error"  
            result.Message = error
            throw error
        }
  
      result.Data = results.rows
      result.Response = "success"
  
      response.status(200).json(result)
    })

}  


module.exports = {
    getHistory,
    getHistoryDay,
    getHistoryHour,
    getHistoryMinute,
    getAllPools,
    getAllPairs,
    getAllPrices,
    getAllTokens,
    getTopTokens,
    getTopTokensByPools,
    getTopTokensRanking,
    getSwapsStats,
    getLastCandle,
    getCoingeckoInfo
  }