from datetime import datetime
import numpy as np
import math
import pandas as pd
from scipy.stats import norm


def bsm(pnow, k, rf, remainingday, yearvol):
    # 输入：（现价,执行价，连续无风险利率3%，剩余天数100天，年波动率0.15）
    # 输出：看涨期权价格[0]，看涨期权delta[1]
    d1 = ((math.log(pnow/k))+(rf+0.5*yearvol*yearvol) *
          (remainingday/255))/(yearvol*math.sqrt(remainingday/255))
    d2 = ((math.log(pnow/k))+(rf-0.5*yearvol*yearvol) *
          (remainingday/255))/(yearvol*math.sqrt(remainingday/255))
    nd1 = norm.cdf(d1)
    nd2 = norm.cdf(d2)
    exprt = math.exp(-rf*remainingday/255)
    prise = pnow*nd1-k*exprt*nd2
    return [prise, nd1]

# getbsm = bsm(3400,3400,0.03,100,0.15)
# print("看涨期权的价格为",getbsm[0],"看涨期权的delta为",getbsm[1])


# BSM参数设定
k = 3400
rf = 0.03  # 连续复利无风险利率
firstremaingday = 93  # 第一天开始交易时剩余交易日，假设期权4月15日到期，距离12月1日还有93个交易日
yearvol = 0.1257

# 其他参数设定
marginrate = 0.1  # 期货保证金率
rr = 0.07  # 年化单利公司资金成本
gap = 0.076  # 组合delta偏差容忍值
feerate = 0.0001  # 期货交易手续费

# 要记录的参数
# 分钟期货交易损益
# 分钟保证金利息

marketinfo = pd.read_excel(pd.ExcelFile('marketinfo.xlsx'), 'Sheet1')

# 记录净值表格
result = pd.DataFrame(columns=('时间', '期权损益', '期货损益', '息前损益', '利息',
                      '息后损益', '净买入期货份额', '剔除时间价值期权损益', '剔除时间价值对冲净收益', '累计期货手续费'))
# 输出交易记录表格
trade = pd.DataFrame(columns=('时间', '期货买入净额', '调整前组合Delta', '调整后组合Delta'))

totalfee = 0  # 统计手续费支出
futureposition = 0
lastprice = marketinfo.iloc[0]['price']
lastbsmprice = bsm(marketinfo.iloc[0]['price'],
                   k, rf, firstremaingday, yearvol)[0]
lastbsmpriceCT = bsm(marketinfo.iloc[0]['price'], k, rf, firstremaingday, yearvol)[
    0]  # 剔除时间价值流逝期权价格
for day in range(0, 22):
    # 21个交易日遍历交易
    for minute in range(0, 345):
        if day == 21 and minute == 225:
            # 12月31日无夜盘
            break
        nowprice = marketinfo.iloc[day*345+minute]['price']
        nowtime = marketinfo.iloc[day*345+minute]['time']
        getbsm = bsm(nowprice, k, rf, firstremaingday-day-minute/345, yearvol)
        bsmpriceCT = bsm(nowprice, k, rf, firstremaingday, yearvol)[
            0]  # 剔除时间价值变化期权价
        optionbsmprice = getbsm[0]
        optionbsmdelta = -getbsm[1]
        profoliodelta = optionbsmdelta + futureposition
        # print("在",nowtime,"时刻的期权价值=",getbsm[0])

        # 结算本分钟的期货头寸和期权头寸的盈亏
        futureprofit = futureposition*(nowprice-lastprice)
        optionprofit = -(optionbsmprice - lastbsmprice)
        netprofitbeforeinterest = futureprofit + optionprofit

        netbuy = 0
        # 调整仓位
        if abs(profoliodelta) > gap:
            netbuy = -profoliodelta
            futureposition = futureposition + netbuy
            totalfee = totalfee + nowprice*feerate*abs(netbuy)
            # 记录交易记录
            trade = trade.append(
                [{'时间': nowtime, '期货买入净额': netbuy, '调整前组合Delta': profoliodelta, '调整后组合Delta': futureposition+optionbsmdelta}])

        # 计算本分钟保证利息占用
        interest = futureposition*nowprice*marginrate*rr/255/345
        netprofitafterinterest = netprofitbeforeinterest - interest

        # 记录本分钟交易数据 '时间','期权损益','期货损益','息前损益','利息','息后损益','净买入期货份额'
        result = result.append([{'时间': nowtime, '期权损益': optionprofit, '期货损益': futureprofit, '息前损益': netprofitbeforeinterest, '利息': interest, '息后损益': netprofitafterinterest,
                               '净买入期货份额': netbuy, '剔除时间价值期权损益': -bsmpriceCT+lastbsmpriceCT, '剔除时间价值对冲净收益': -bsmpriceCT+lastbsmpriceCT+futureprofit-interest, '累计期货手续费': totalfee}], ignore_index=True)

        lastprice = nowprice
        lastbsmprice = optionbsmprice  # 记录本分钟bsm价格给下一分钟使用
        lastbsmpriceCT = bsm(nowprice, k, rf, firstremaingday, yearvol)[0]
        # print(marketinfo.iloc[day*345+minute]['time'])

# 导出结果
result.to_excel('result.xlsx')
trade.to_excel('trade.xlsx')
————————————————
# 版权声明：本文为CSDN博主「Maple丶峰」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
# 原文链接：https://blog.csdn.net/lvlinfeng970/article/details/105154964


# 参数设定
minute = 200000  # 模拟多少分钟
feerate = 0.0001
# 期权参数
pnow = 3400
k = 3400
rf = 0.03
remainingday = 93
yearvol = 0.1257

# 获取期权gamma值


def getgamma(pnow, k, rf, remainingday, yearvol):
    d1 = ((math.log(pnow/k))+(rf+0.5*yearvol*yearvol) *
          (remainingday/255))/(yearvol*math.sqrt(remainingday/255))
    gamma = np.exp(-0.5*d1*d1)/(pnow*yearvol*np.sqrt(2*np.pi*remainingday/255))
    return gamma


gamma = getgamma(pnow, k, rf, remainingday, yearvol)
difdelta = pd.read_excel(pd.ExcelFile('delta差分序列.xlsx'), 'Sheet1')
result = pd.DataFrame(columns=('阈值', 'gamma损失', '交易次数', '交易手续费损失', '总期望损失'))
for gap in range(1, 101):
    gap = gap/1000
    lose = 0  # 记录损失数
    hedge = 0  # 记录对冲次数
    nowplace = 0
    for i in range(0, minute):
        nowplace = nowplace + difdelta['difdelta'][np.random.randint(0, 7468)]
        if np.abs(nowplace) > gap:
            changep = nowplace / gamma
            lose = lose + 0.5*gamma*changep*changep
            nowplace = 0
            hedge = hedge + 1
        if range == minute-1 and nowplace != 0:
            # 最后一分钟结转所有损失
            lose = lose + 0.5*gamma*changep*changep

    result = result.append([{'阈值': gap, 'gamma损失': lose, '交易次数': hedge,
                           '交易手续费损失': hedge*pnow*feerate, '总期望损失': hedge*pnow*feerate+lose}])
result.to_excel('寻找最优阈值.xlsx')
————————————————
# 版权声明：本文为CSDN博主「Maple丶峰」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
# 原文链接：https://blog.csdn.net/lvlinfeng970/article/details/105154964


def implied_volatility_call(S, K, T, r, c):
    implied_vol = 1.0
    min_value = 100.0
    tts = 0.0001
    for i in range(0, 1000):
        sigma = (implied_vol-tts*i)
        d1 = (np.lib.scimath.log(S/K)+(r+sigma*sigma/2.0)*T) / \
            (sigma*np.lib.scimath.sqrt(T))
        d2 = d1-sigma*np.lib.scimath.sqrt(T)
        call = S*stats.norm.cdf(d1)-K*np.exp(-r*T)*stats.norm.cdf(d2)
        abs_diff = abs(call-c)
        if abs_diff <= min_value:
            min_value = abs_diffimplied_vol = sigma

    return implied_vol*100

mport numpy as np
import scipy.stats as si

def d(s,k,r,T,sigma):
    d1 = (np.log(s / k) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return (d1,d2)

def delta(s,k,r,T,sigma,n):
	'''
    认购期权的n为1
    认沽期权的n为-1
	'''
	d1 = d(s,k,r,T,sigma)[0]
    delta = n * si.norm.cdf(n * d1)
    return delta

def gamma(s,k,r,T,sigma):
	d1 = d(s,k,r,T,sigma)[0]
    gamma = si.norm.pdf(d1) / (s * sigma * np.sqrt(T))
    return gamma

def vega(s,k,r,T,sigma):
	d1 = d(s,k,r,T,sigma)[0]
    vega = (s * si.norm.pdf(d1) * np.sqrt(T)) / 100
    return vega

def theta(s,k,r,T,sigma,n):
	'''
    认购期权的n为1
    认沽期权的n为-1
	'''
	d1 = d(s,k,r,T,sigma)[0]
	d2 = d(s,k,r,T,sigma)[1]

    theta = (-1 * (s * si.norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - n * r * k * np.exp(-r * T) * si.norm.cdf(n * d2)) / 365
    return theta
————————————————
# 版权声明：本文为CSDN博主「Zita_11」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
# 原文链接：https://blog.csdn.net/Zita_11/article/details/104200887

if __name__ == "__main__":
    nowdt = datetime.now()
    enddate = datetime.strptime('2021-8-11 13:30:00', '%Y-%m-%d %H:%M:%S')
    T = ((enddate-nowdt).days + (enddate - nowdt).seconds/3600.0/24)/225.0
    rate = 0.01
    sigma = implied_volatility_call(17603.12, 17600, T, rate, 117)
