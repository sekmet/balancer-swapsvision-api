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

const getLiquidityStats = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 3000
  pool.query("SELECT count(id) AS total_transactions, tokensym_in, pool_id, last(pool_liquidity, time) AS total_liquidity,  SUM(amount_in) AS total_in, SUM(amount_out) AS total_out, SUM(amount) as total_amount, SUM(fee) as total_fee, date_part('day', time) AS swap_day, date_part('month', time) AS swap_month, date_part('year', time) AS swap_year FROM api.swaps WHERE date_part('day', time) = '1' GROUP BY tokensym_in, pool_id, swap_day, swap_month, swap_year ORDER BY tokensym_in ASC, swap_year DESC, swap_month DESC, swap_day DESC LIMIT $1", [_limit], (error, results) => {
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


const getDailyLiquidityStats = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 365
  pool.query("WITH swaps AS (SELECT date_trunc('day', time) AS day, COUNT(*) AS txns, SUM(amount) AS volume FROM api.swaps GROUP BY 1 ) SELECT day, txns AS swaps, volume, volume/txns AS avgvolswap FROM swaps WHERE date_part('year', day) = '2021' ORDER BY day ASC LIMIT $1", [_limit], (error, results) => {
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
  pool.query("select sum(liquidity) as total_liquidity, array_agg(jsonb_build_object('tokens', tokens_info)) AS pair, tokens_list from api.pools group by tokens_list order by total_liquidity DESC LIMIT $1", [_limit], (error, results) => {
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

  pool.query(`SELECT id, name, symbol, image, total_balance, (total_balance*market_price) AS liquidity, total_volume, price_usdc, price_dai, price_weth, market_price, price_change_percentage_24h AS percent24hs, price_change_24h AS change24hs from api.prices WHERE market_price > 0 ORDER BY ${_sortby} DESC LIMIT $1 OFFSET $2`, [_limit, _skip], (error, results) => {
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
  pool.query("SELECT count(id) AS total_transactions, SUM(pool_liquidity) as total_liquidity, SUM(amount) as total_amount, SUM(fee) as total_fee, pair, AVG(amount::numeric/amount_in::numeric) AS avg_price, MAX(time) AS last_swap, time_bucket('1 day', time) AS tb FROM api.swaps WHERE date_part('day', time) = date_part('day', NOW()) AND date_part('month', time) = date_part('month', NOW()) AND date_part('year', time) = date_part('year', NOW()) GROUP BY pair,tb LIMIT $1", [_limit], (error, results) => {
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

const getTopPairsRanking = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 1000
  let _skip = 0
  let _orderby = request.params.sortby ? request.params.sortby : 'liquidity'
  let _orderdirection = request.params.sortdirection ? request.params.sortdirection : 'DESC'
  if (request.params.limit > 10){
    _skip = request.params.limit - 10
    _limit = 10
  }  
  pool.query(`SELECT * from api.toppairs WHERE transactions_24hs > 0 ORDER BY ${_orderby} ${_orderdirection} LIMIT $1 OFFSET ${_skip}`, [_limit], (error, results) => {
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

const getAllSwaps = (request, response) => {
  let result = {}
  let _limit = request.params.limit ? request.params.limit: 10
  let _skip = 0
  let _orderby = request.params.orderby ? request.params.orderby : 'time'
  let _orderdirection = request.params.orderdirection ? request.params.orderdirection : 'DESC'
  if (request.params.limit > 10){
  _skip = request.params.limit - 10
  _limit = 10
  }

  pool.query(`SELECT * FROM api.swaps ORDER BY ${_orderby} ${_orderdirection} LIMIT $1 OFFSET $2`, [_limit, _skip], (error, results) => {
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
    pool.query("SELECT * from api.prices WHERE market_price > 0 AND total_balance > 0 ORDER BY market_cap DESC LIMIT $1", [_limit], (error, results) => {
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
  pool.query("SELECT symbol from api.prices WHERE market_price > 0 AND total_balance > 0 LIMIT $1", [_limit], (error, results) => {
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
  if (resolution && (resolution.includes('D') || resolution === '1D' || resolution === 'D')) {
    _resolution = `1440 minute`
  } else if (parseInt(resolution, 10) > 720) {
    _resolution = `1440 minute`
  } else {
    _resolution = `${parseInt(resolution, 10)} minute`
  }

  console.log(resolution, _resolution, fsym, tsym, toTs, limit, orderby, orderdirection)
  let _toTs = toTs ? toTs : Date.now()
  let _limit = 1
  let _orderby = orderby ? orderby : 'tb'
  let _orderdirection = orderdirection ? orderdirection : 'ASC'

  pool.query(`SELECT time_bucket('${_resolution}', time) as tb, first((amount::numeric/amount_in::numeric), time) AS open, last((amount::numeric/amount_in::numeric), time) AS close, MAX(amount::numeric/amount_in::numeric) AS high, MIN(amount::numeric/amount_in::numeric) AS low, SUM(amount) AS volume, (amount::numeric/amount_in::numeric) AS price FROM api.swaps WHERE pair=$1 GROUP BY tb, pair, amount, amount_in, price ORDER BY ${_orderby} ${_orderdirection} LIMIT 1`, [currentPair], (error, results) => {
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

  pool.query(`SELECT time_bucket('${_resolution}', time) as tb, first((amount::numeric/amount_in::numeric), time) AS open, last((amount::numeric/amount_in::numeric), time) AS close, MAX(amount::numeric/amount_in::numeric) AS high, MIN(amount::numeric/amount_in::numeric) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
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

  pool.query(`SELECT time_bucket('1 day', time) as tb, first((amount::numeric/amount_in::numeric), time) AS open, last((amount::numeric/amount_in::numeric), time) AS close, MAX(price) AS high, MIN(price) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
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

    pool.query(`SELECT time_bucket('4 hour', time) as tb, first((amount::numeric/amount_in::numeric), time) AS open,last((amount::numeric/amount_in::numeric), time) AS close, MAX(amount::numeric/amount_in::numeric) AS high, MIN(amount::numeric/amount_in::numeric) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
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

    pool.query(`SELECT time_bucket('1 minute', time) as tb, first((amount::numeric/amount_in::numeric), time) AS open, last((amount::numeric/amount_in::numeric), time) AS close, MAX(amount::numeric/amount_in::numeric) AS high, MIN(amount::numeric/amount_in::numeric) AS low, SUM(amount) AS volume FROM api.swaps WHERE pair=$1 GROUP BY tb, pair ORDER BY ${_orderby} ${_orderdirection} LIMIT ${_limit}`, [currentPair], (error, results) => {
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
    getAllSwaps,
    getAllPools,
    getAllPairs,
    getAllPrices,
    getAllTokens,
    getTopTokens,
    getTopTokensByPools,
    getTopTokensRanking,
    getTopPairsRanking,
    getSwapsStats,
    getLastCandle,
    getLiquidityStats,
    getDailyLiquidityStats,
    getCoingeckoInfo
  }