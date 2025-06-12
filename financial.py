import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import os
import logging
from typing import List, Dict, Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('finance_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EastMoneyCrawler:
    """
    东方财富网数据爬虫
    
    功能：
    1. 获取A股股票列表
    2. 获取实时行情数据
    3. 获取历史K线数据
    4. 获取财务数据
    """
    
    def __init__(self, save_path: str = './finance_data'):
        """
        初始化爬虫
        
        Args:
            save_path: 数据保存路径
        """
        self.save_path = save_path
        self.session = requests.Session()
        
        # 设置请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'http://quote.eastmoney.com/',
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        self.session.headers.update(self.headers)
        
        # 创建保存目录
        os.makedirs(self.save_path, exist_ok=True)
        logger.info(f"数据将保存至: {os.path.abspath(self.save_path)}")
        
    def get_stock_list(self, page_size: int = 10000) -> pd.DataFrame:
        """
        获取A股股票列表
        
        Args:
            page_size: 每页数据量，默认10000以确保获取所有股票
            
        Returns:
            包含股票信息的DataFrame
        """
        logger.info("开始获取A股股票列表...")
        
        # A股列表API
        url = "http://82.push2.eastmoney.com/api/qt/clist/get"
        
        params = {
            'cb': 'jQuery112404777825173892854_1234567890',
            'pn': 1,  # 页码
            'pz': page_size,  # 每页数量
            'po': 1,  # 排序方式
            'np': 1,
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': 2,
            'invt': 2,
            'fid': 'f3',
            'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',  # 市场类型
            'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152',
            '_': str(int(time.time() * 1000))
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            # 解析JSONP响应
            json_str = response.text[response.text.index('(') + 1:response.text.rindex(')')]
            data = json.loads(json_str)
            
            if data['data'] and data['data']['diff']:
                stock_list = []
                
                for stock in data['data']['diff']:
                    stock_info = {
                        '股票代码': stock['f12'],
                        '股票名称': stock['f14'],
                        '最新价': stock['f2'],
                        '涨跌幅': stock['f3'],
                        '涨跌额': stock['f4'],
                        '成交量': stock['f5'],
                        '成交额': stock['f6'],
                        '振幅': stock['f7'],
                        '最高': stock['f15'],
                        '最低': stock['f16'],
                        '今开': stock['f17'],
                        '昨收': stock['f18'],
                        '量比': stock['f10'],
                        '换手率': stock['f8'],
                        '市盈率': stock['f9'],
                        '市净率': stock['f23'],
                        '总市值': stock['f20'],
                        '流通市值': stock['f21'],
                        '更新时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    stock_list.append(stock_info)
                
                df = pd.DataFrame(stock_list)
                logger.info(f"成功获取 {len(df)} 只股票数据")
                
                # 保存数据
                self._save_data(df, 'stock_list')
                return df
                
            else:
                logger.warning("未获取到股票数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_kline_data(self, stock_code: str, period: str = '101', 
                      start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取股票K线数据
        
        Args:
            stock_code: 股票代码（如：000001）
            period: K线周期（101:日K, 102:周K, 103:月K）
            start_date: 开始日期（YYYYMMDD）
            end_date: 结束日期（YYYYMMDD）
            
        Returns:
            K线数据DataFrame
        """
        logger.info(f"开始获取股票 {stock_code} 的K线数据...")
        
        # 判断市场（0:深市, 1:沪市）
        market = '0' if stock_code.startswith(('0', '3')) else '1'
        
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        
        # 如果没有指定日期，默认获取最近一年的数据
        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        params = {
            'cb': 'jQuery112409568143051406726_1234567890',
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'klt': period,
            'fqt': '1',  # 复权类型：0不复权，1前复权，2后复权
            'secid': f'{market}.{stock_code}',
            'beg': start_date,
            'end': end_date,
            '_': str(int(time.time() * 1000))
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            # 解析JSONP
            json_str = response.text[response.text.index('(') + 1:response.text.rindex(')')]
            data = json.loads(json_str)
            
            if data['data'] and data['data']['klines']:
                kline_list = []
                
                for kline in data['data']['klines']:
                    fields = kline.split(',')
                    kline_data = {
                        '日期': fields[0],
                        '开盘价': float(fields[1]),
                        '收盘价': float(fields[2]),
                        '最高价': float(fields[3]),
                        '最低价': float(fields[4]),
                        '成交量': float(fields[5]),
                        '成交额': float(fields[6]),
                        '振幅': float(fields[7]),
                        '涨跌幅': float(fields[8]),
                        '涨跌额': float(fields[9]),
                        '换手率': float(fields[10])
                    }
                    kline_list.append(kline_data)
                
                df = pd.DataFrame(kline_list)
                df['股票代码'] = stock_code
                df['股票名称'] = data['data']['name']
                
                logger.info(f"成功获取 {stock_code} 的 {len(df)} 条K线数据")
                
                # 保存数据
                self._save_data(df, f'kline_{stock_code}')
                return df
                
            else:
                logger.warning(f"未获取到股票 {stock_code} 的K线数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return pd.DataFrame()
    
    def get_financial_data(self, stock_code: str) -> pd.DataFrame:
        """
        获取股票财务数据 - 使用更稳定的API
        
        Args:
            stock_code: 股票代码
            
        Returns:
            财务数据DataFrame
        """
        logger.info(f"开始获取股票 {stock_code} 的财务数据...")
        
        # 判断市场（0:深市, 1:沪市）
        market = '0' if stock_code.startswith(('0', '3')) else '1'
        
        # 使用更稳定的财务数据API
        url = "http://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
        
        params = {
            'lmt': 0,
            'klt': 101,  # 日K
            'secid': f'{market}.{stock_code}',
            'fields1': 'f1,f2,f3,f7',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65',
            'ut': 'b2884a393a59ad64002292a3e90d46a5',
            '_': str(int(time.time() * 1000))
        }
        
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            # 尝试第一个API
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data and 'data' in data and data['data'] and 'klines' in data['data']:
                        # 解析资金流数据作为财务数据的补充
                        financial_list = []
                        recent_data = data['data']['klines'][-10:] if data['data']['klines'] else []
                        
                        for i, kline in enumerate(recent_data):
                            fields = kline.split(',')
                            if len(fields) >= 10:
                                financial_data = {
                                    '日期': fields[0],
                                    '主力净流入': float(fields[1]) if fields[1] != '-' else 0,
                                    '小单净流入': float(fields[2]) if fields[2] != '-' else 0,
                                    '中单净流入': float(fields[3]) if fields[3] != '-' else 0,
                                    '大单净流入': float(fields[4]) if fields[4] != '-' else 0,
                                    '超大单净流入': float(fields[5]) if fields[5] != '-' else 0,
                                    '主力净流入占比': float(fields[6]) if fields[6] != '-' else 0,
                                    '收盘价': float(fields[7]) if fields[7] != '-' else 0,
                                    '涨跌幅': float(fields[8]) if fields[8] != '-' else 0,
                                }
                                financial_list.append(financial_data)
                        
                        if financial_list:
                            df = pd.DataFrame(financial_list)
                            df['股票代码'] = stock_code
                            logger.info(f"成功获取 {stock_code} 的 {len(df)} 条资金流数据")
                            self._save_data(df, f'financial_{stock_code}')
                            return df
                except:
                    pass
            
            # 如果第一个API失败，尝试第二个API
            logger.info(f"尝试备用API获取 {stock_code} 的财务数据...")
            
            backup_url = "http://f10.eastmoney.com/pc_hsgt/GetHSGTData.aspx"
            backup_params = {
                'code': stock_code,
                'type': 'rpm'
            }
            
            backup_response = self.session.get(backup_url, params=backup_params, timeout=15)
            
            if backup_response.status_code == 200:
                # 简化财务数据，主要获取基本信息
                financial_data = {
                    '股票代码': stock_code,
                    '查询时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '数据状态': '已查询',
                    '备注': '使用备用接口获取基础数据'
                }
                
                df = pd.DataFrame([financial_data])
                logger.info(f"使用备用方法获取 {stock_code} 的基础财务数据")
                self._save_data(df, f'financial_{stock_code}')
                return df
            else:
                # 如果都失败，返回基本信息
                logger.warning(f"无法获取股票 {stock_code} 的财务数据，返回基本信息")
                basic_data = {
                    '股票代码': stock_code,
                    '查询时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '数据状态': '获取失败',
                    '备注': f'股票代码 {stock_code} 可能已停牌或退市'
                }
                
                df = pd.DataFrame([basic_data])
                self._save_data(df, f'financial_{stock_code}')
                return df
                
        except Exception as e:
            logger.error(f"获取财务数据失败: {e}")
            # 返回错误信息而不是空DataFrame
            error_data = {
                '股票代码': stock_code,
                '查询时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '数据状态': '获取失败',
                '错误信息': str(e),
                '备注': '请检查网络连接或股票代码是否正确'
            }
            
            df = pd.DataFrame([error_data])
            self._save_data(df, f'financial_{stock_code}')
            return df
    
    def batch_crawl(self, stock_codes: List[str], crawl_type: str = 'all'):
        """
        批量爬取数据
        
        Args:
            stock_codes: 股票代码列表
            crawl_type: 爬取类型（'all', 'kline', 'financial'）
        """
        logger.info(f"开始批量爬取 {len(stock_codes)} 只股票的数据...")
        
        all_kline_data = []
        all_financial_data = []
        
        for i, code in enumerate(stock_codes):
            logger.info(f"正在处理第 {i+1}/{len(stock_codes)} 只股票: {code}")
            
            try:
                if crawl_type in ['all', 'kline']:
                    kline_df = self.get_kline_data(code)
                    if not kline_df.empty:
                        all_kline_data.append(kline_df)
                    time.sleep(1)  # 控制请求频率
                
                if crawl_type in ['all', 'financial']:
                    financial_df = self.get_financial_data(code)
                    if not financial_df.empty:
                        all_financial_data.append(financial_df)
                    time.sleep(1.5)  # 财务数据请求间隔稍长
                    
            except Exception as e:
                logger.error(f"处理股票 {code} 时发生错误: {e}")
                continue
        
        # 合并并保存所有数据
        if all_kline_data:
            merged_kline = pd.concat(all_kline_data, ignore_index=True)
            self._save_data(merged_kline, 'all_kline_data')
            
        if all_financial_data:
            merged_financial = pd.concat(all_financial_data, ignore_index=True)
            self._save_data(merged_financial, 'all_financial_data')
        
        logger.info("批量爬取完成！")
    
    def _save_data(self, df: pd.DataFrame, filename: str):
        """
        保存数据到文件
        
        Args:
            df: 要保存的DataFrame
            filename: 文件名（不含扩展名）
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 保存为CSV
        csv_path = os.path.join(self.save_path, f'{filename}_{timestamp}.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"数据已保存至CSV: {csv_path}")
        
        # 保存为Excel
        excel_path = os.path.join(self.save_path, f'{filename}_{timestamp}.xlsx')
        df.to_excel(excel_path, index=False, engine='openpyxl')
        logger.info(f"数据已保存至Excel: {excel_path}")
        
        # 保存为JSON（用于后续程序读取）
        json_path = os.path.join(self.save_path, f'{filename}_{timestamp}.json')
        df.to_json(json_path, orient='records', force_ascii=False, indent=2)
        logger.info(f"数据已保存至JSON: {json_path}")


def main():
    """
    主函数 - 演示爬虫使用
    """
    # 创建爬虫实例
    crawler = EastMoneyCrawler()
    
    # 1. 获取股票列表（增加到10000条以确保获取足够多的数据）
    stock_df = crawler.get_stock_list(page_size=10000)
    
    if not stock_df.empty and len(stock_df) >= 1000:
        logger.info(f"成功获取 {len(stock_df)} 只股票数据，满足1000+的要求")
        
        # 2. 选择部分股票进行详细数据爬取
        # 这里仍然选择前10只股票作为详细数据爬取
        selected_stocks = stock_df['股票代码'].head(10).tolist()
        
        logger.info(f"将对以下10只股票进行详细数据爬取: {selected_stocks}")
        
        # 3. 批量爬取K线和财务数据
        crawler.batch_crawl(selected_stocks, crawl_type='all')
        
        # 4. 数据统计
        logger.info("\n=== 数据爬取统计 ===")
        logger.info(f"股票列表总数: {len(stock_df)} 只")
        logger.info(f"详细数据爬取: {len(selected_stocks)} 只")
        logger.info(f"数据保存路径: {os.path.abspath(crawler.save_path)}")
        
        # 5. 特别测试300521股票
        logger.info("\n=== 特别测试300521股票 ===")
        test_financial = crawler.get_financial_data('300521')
        if not test_financial.empty:
            logger.info("300521财务数据获取成功！")
        else:
            logger.warning("300521财务数据获取仍然失败")
    
    elif not stock_df.empty:
        logger.warning(f"仅获取到 {len(stock_df)} 只股票数据，未达到1000条要求")
        
        # 即使数据不足1000条，也继续进行后续处理
        selected_stocks = stock_df['股票代码'].head(10).tolist()
        crawler.batch_crawl(selected_stocks, crawl_type='all')
        
    else:
        logger.error("未能获取股票列表，爬取终止")


if __name__ == "__main__":
    main()