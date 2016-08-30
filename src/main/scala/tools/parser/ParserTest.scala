package tools.parser

import scala.collection.immutable.HashMap

/**
  * Created by YangShuai
  * Created on 2016/8/24.
  */
object ParserTest {

  def main(args: Array[String]) {

    val s = "1:总股本小于12亿+1:流通比例大于3%小于2%+1:户均持股数大于2万小于6万+1:大股东减股+2:涨跌幅大于1%小于2%+2:振幅等于12%+3:资金流入大于8万+4:连续5天被3~1个大V看好+4:查看热度连续7天出现在top10"

    println(Query.parser(s))
  }

  val QUERYMAP = {

    HashMap(
      (1, HashMap(

        // 包含大于小于的语句的操作码
        ("总股本大于X亿", 101),
        ("总股本小于X亿", 101),
        ("总股本等于X亿", 101),
        ("总股本大于X亿小于X亿", 101),
        ("流通股本大于X亿", 102),
        ("流通股本小于X亿", 102),
        ("流通股本等于X亿", 102),
        ("流通股本大于X亿小于X亿", 102),
        ("总市值大于X亿", 103),
        ("总市值小于X亿", 103),
        ("总市值等于X亿", 103),
        ("总市值大于X亿小于X亿", 103),
        ("流通市值大于X亿", 104),
        ("流通市值小于X亿", 104),
        ("流通市值等于X亿", 104),
        ("流通市值大于X亿小于X亿", 104),
        ("流通比例大于X%", 105),
        ("流通比例小于X%", 105),
        ("流通比例等于X%", 105),
        ("流通比例大于X%小于X%", 105),
        ("十大股东持股比例大于X%", 106),
        ("十大股东持股比例小于X%", 106),
        ("十大股东持股比例等于X%", 106),
        ("十大股东持股比例大于X%小于X%", 106),
        ("股东户数大于X万", 107),
        ("股东户数小于X万", 107),
        ("股东户数等于X万", 107),
        ("股东户数大于X万小于X万", 107),
        ("户均持股数大于X万", 108),
        ("户均持股数小于X万", 108),
        ("户均持股数等于X万", 108),
        ("户均持股数大于X万小于X万", 108),

        // 非大于小于类型的数据操作码
        // 增减持
        ("大股东增股", 40001),
        ("大股东减股", 40001),
        ("高管增股", 40001),
        ("高管减股", 40001),

        // 是否持股
        ("基金持股", 40001),
        ("券商持股", 40001),
        ("社保持股", 40001),
        ("信托持股", 40001),
        ("保险持股", 40001),
        ("QFII持股", 40001),
        ("国家队持股", 40001))),

      (2, HashMap(

        // 大于小于类型的查询条件
        // 涨跌幅
        ("涨跌幅大于X%", 201),
        ("涨跌幅小于X%", 201),
        ("涨跌幅等于X%", 201),
        ("涨跌幅大于X%小于X%", 201),
        ("涨幅大于X%", 202),
        ("涨幅小于X%", 202),
        ("涨幅等于X%", 202),
        ("涨幅大于X%小于X%", 202),
        ("跌幅大于X%", 203),
        ("跌幅小于X%", 203),
        ("跌幅等于X%", 203),
        ("跌幅大于X%小于X%", 203),

        //振幅
        ("振幅大于X%", 204),
        ("振幅小于X%", 204),
        ("振幅等于X%", 204),
        ("振幅大于X%小于X%", 204),

        //换手率
        ("换手率大于X%", 205),
        ("换手率小于X%", 205),
        ("换手率等于X%", 205),
        ("换手率大于X%小于X%", 205),

        //成交量
        ("成交量大于X万", 206),
        ("成交量小于X万", 206),
        ("成交量等于X万", 206),
        ("成交量大于X万小于X万", 206),

        //成交额
        ("成交额大于X万", 207),
        ("成交额小于X万", 207),
        ("成交额等于X万", 207),
        ("成交额大于X万小于X万", 207),

        //股价
        ("股价大于X元", 208),
        ("股价小于X元", 208),
        ("股价等于X元", 208),
        ("股价大于X元小于X元", 208),

        //收益率
        ("收益率大于X%", 209),
        ("收益率小于X%", 209),
        ("收益率等于X%", 209),
        ("收益率大于X小于X%", 209))),

      (3, HashMap(

        // 大于小于类型的查询条件
        //资金流入
        ("资金流入大于X万", 301),
        ("资金流入小于X万", 301),
        ("资金流入等于X万", 301),
        ("资金流入大于X万小于X万", 301))),

      (4, HashMap(

        // 大于小于类型的查询条件
        //新闻访问热度
        ("新闻访问热度每天大于X次", 401),
        ("新闻访问热度每天小于X次", 401),
        ("新闻访问热度每天等于X次", 401),
        ("新闻访问热度每天大于X次小于X次", 401),
        ("新闻访问热度每周大于X次", 2401),
        ("新闻访问热度每周小于X次", 2401),
        ("新闻访问热度每周等于X次", 2401),
        ("新闻访问热度每周大于X次小于X次", 2401),
        ("新闻访问热度每月大于X次", 4401),
        ("新闻访问热度每月小于X次", 4401),
        ("新闻访问热度每月等于X次", 4401),
        ("新闻访问热度每月大于X次小于X次", 4401),
        ("新闻访问热度每年大于X次", 8401),
        ("新闻访问热度每年小于X次", 8401),
        ("新闻访问热度每年等于X次", 8401),
        ("新闻访问热度每年大于X次小于X次", 8401),

        //新闻转载热度
        ("新闻转载热度每天大于X次", 402),
        ("新闻转载热度每天小于X次", 402),
        ("新闻转载热度每天等于X次", 402),
        ("新闻转载热度每天大于X次小X次", 402),
        ("新闻转载热度每周大于X次", 2402),
        ("新闻转载热度每周小于X次", 2402),
        ("新闻转载热度每周等于X次", 2402),
        ("新闻转载热度每周大于X次小于X次", 2402),
        ("新闻转载热度每月大于X次", 4402),
        ("新闻转载热度每月小于X次", 4402),
        ("新闻转载热度每月等于X次", 4402),
        ("新闻转载热度每月大于X次小于X次", 4402),
        ("新闻转载热度每年大于X次", 8402),
        ("新闻转载热度每年小于X次", 8402),
        ("新闻转载热度每年等于X次", 8402),
        ("新闻转载热度每年大于X次小于X次", 8402),

        // 非大于小于类型的查询条件
        //公告性事件
        ("盈利预增X%", 40001),
        ("诉讼仲裁X次", 40002),
        ("违规处罚X次", 40003),
        ("盈利预增X%以上", 40001),
        ("诉讼仲裁X次以上", 40002),
        ("违规处罚X次以上", 40003),

        //新闻趋势
        ("新闻趋势连续X天上涨", 10001),
        ("新闻趋势连续X天下降", 10002),
        ("新闻趋势连续X天以上上涨", 10001),
        ("新闻趋势连续X天以上下降", 10002),

        //新闻情感
        ("新闻情感连续X天都是非负面情绪", 10003),
        ("新闻情感连续X天都是负面情绪", 10004),
        ("新闻情感连续X天以上都是非负面情绪", 10003),
        ("新闻情感连续X天以上都是负面情绪", 10004),

        //大V观点
        ("连续X天被X个大V看好", 10005),
        ("连续X天被X个大V看空", 10006),
        ("连续X天以上被X个大V看好", 10005),
        ("连续X天以上被X个大V看空", 10006),
        ("连续X天被X个大V以上看好", 10005),
        ("连续X天被X个大V以上看空", 10006),
        ("连续X~X天被X个大V看好", 10005),
        ("连续X~X天被X个大V看空", 10006),
        ("连续X天被X~X个大V看好", 10005),
        ("连续X天被X~X个大V看空", 10006),

        //行为数据
        ("查看热度连续X天上涨超过X", 10007),
        ("查看热度连续X天出现在topX", 10008),
        ("查看热度连续X天以上上涨超过X", 10007),
        ("查看热度连续X天以上出现在topX", 10008),
        ("查看热度连续X天超过X", 10009),
        ("查看热度连续X天以上超过X", 10009))))
  }
}