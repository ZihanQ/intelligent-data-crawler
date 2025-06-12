import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import re
from datetime import datetime
import os
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('medical_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MedicalDataCrawler:
    """
    医疗数据爬虫
    
    功能：
    1. 爬取国家卫健委统计数据
    2. 爬取各省市医院信息
    3. 爬取疾病防控数据
    4. 爬取医疗政策文件
    """
    
    def __init__(self, save_path: str = './medical_data'):
        """
        初始化爬虫
        
        Args:
            save_path: 数据保存路径
        """
        self.save_path = save_path
        self.session = requests.Session()
        
        # 设置请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.session.headers.update(self.headers)
        
        # 创建保存目录
        os.makedirs(self.save_path, exist_ok=True)
        os.makedirs(os.path.join(self.save_path, 'documents'), exist_ok=True)
        logger.info(f"数据将保存至: {os.path.abspath(self.save_path)}")
        
    def get_nhc_statistics(self, category: str = 'ylfwygl') -> pd.DataFrame:
        """
        获取国家卫健委统计数据
        
        Args:
            category: 数据类别
                - ylfwygl: 医疗服务与管理
                - wsjkkj: 卫生健康科技
                - jbkzzx: 疾病控制中心数据
                
        Returns:
            统计数据DataFrame
        """
        logger.info(f"开始获取国家卫健委统计数据，类别: {category}")
        
        base_url = "http://www.nhc.gov.cn/mohwsbwstjxxzx"
        category_urls = {
            'ylfwygl': f"{base_url}/s7967/list.shtml",  # 医疗服务管理
            'wsjkkj': f"{base_url}/s7966/list.shtml",   # 卫生健康科技
            'jbkzzx': f"{base_url}/s7965/list.shtml"    # 疾病控制
        }
        
        url = category_urls.get(category, category_urls['ylfwygl'])
        all_data = []
        
        try:
            # 获取列表页
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找数据列表
            list_items = soup.select('ul.zxxx_list li')
            
            for item in list_items[:20]:  # 获取最新20条
                try:
                    link_elem = item.select_one('a')
                    date_elem = item.select_one('span.ml')
                    
                    if link_elem:
                        title = link_elem.text.strip()
                        href = link_elem.get('href', '')
                        
                        # 处理相对URL
                        if href.startswith('/'):
                            href = f"http://www.nhc.gov.cn{href}"
                        elif not href.startswith('http'):
                            href = urljoin(url, href)
                        
                        date_str = date_elem.text.strip() if date_elem else ''
                        
                        # 获取详细内容
                        detail_data = self._get_detail_content(href, title)
                        
                        data = {
                            '标题': title,
                            '发布日期': date_str,
                            '链接': href,
                            '类别': category,
                            **detail_data
                        }
                        all_data.append(data)
                        
                        time.sleep(1)  # 控制请求频率
                        
                except Exception as e:
                    logger.error(f"处理列表项失败: {e}")
                    continue
            
            if all_data:
                df = pd.DataFrame(all_data)
                logger.info(f"成功获取 {len(df)} 条数据")
                
                # 保存数据
                self._save_data(df, f'nhc_statistics_{category}')
                return df
            else:
                logger.warning("未获取到数据")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取统计数据失败: {e}")
            return pd.DataFrame()
    
    def get_hospital_data(self, province: str = '北京') -> pd.DataFrame:
        """
        获取医院信息数据
        
        Args:
            province: 省份名称
            
        Returns:
            医院信息DataFrame
        """
        logger.info(f"开始获取 {province} 的医院数据...")
        
        # 这里使用一个示例API，实际项目中需要根据具体网站调整
        # 可以爬取各省卫健委网站或医院查询平台
        
        hospitals = []
        
        # 示例：爬取医院基本信息
        # 这里需要根据实际的数据源进行调整
        try:
            # 假设有一个医院查询接口
            url = f"https://example.com/api/hospitals"
            params = {
                'province': province,
                'page': 1,
                'size': 100
            }
            
            # 实际爬取时需要找到真实的数据源
            # response = self.session.get(url, params=params)
            # data = response.json()
            
            # 模拟数据结构
            sample_hospitals = [
                {
                    '医院名称': f'{province}市第一人民医院',
                    '医院等级': '三级甲等',
                    '医院类型': '综合医院',
                    '床位数': 1500,
                    '地址': f'{province}市中心区',
                    '电话': '010-12345678'
                },
                {
                    '医院名称': f'{province}中医医院',
                    '医院等级': '三级甲等',
                    '医院类型': '中医医院',
                    '床位数': 800,
                    '地址': f'{province}市东区',
                    '电话': '010-87654321'
                }
            ]
            
            hospitals.extend(sample_hospitals)
            
            if hospitals:
                df = pd.DataFrame(hospitals)
                df['省份'] = province
                df['更新时间'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                logger.info(f"成功获取 {len(df)} 家医院数据")
                self._save_data(df, f'hospitals_{province}')
                return df
                
        except Exception as e:
            logger.error(f"获取医院数据失败: {e}")
            return pd.DataFrame()
    
    def get_disease_statistics(self) -> pd.DataFrame:
        """
        获取疾病统计数据（如传染病疫情数据）
        
        Returns:
            疾病统计DataFrame
        """
        logger.info("开始获取疾病统计数据...")
        
        # 中国疾病预防控制中心数据
        url = "http://www.chinacdc.cn/jkzt/crb/zl/szkb_11803/jszl_11809/"
        
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            disease_data = []
            
            # 查找疫情通报列表
            reports = soup.select('ul.list li')
            
            for report in reports[:10]:  # 获取最新10条
                try:
                    link = report.select_one('a')
                    if link:
                        title = link.text.strip()
                        href = link.get('href', '')
                        
                        # 处理URL
                        if not href.startswith('http'):
                            href = urljoin(url, href)
                        
                        # 提取日期
                        date_match = re.search(r'(\d{4})年(\d{1,2})月', title)
                        if date_match:
                            year, month = date_match.groups()
                            report_date = f"{year}-{month.zfill(2)}"
                        else:
                            report_date = ''
                        
                        # 获取详细数据
                        detail_data = self._get_disease_detail(href)
                        
                        data = {
                            '报告标题': title,
                            '报告日期': report_date,
                            '链接': href,
                            **detail_data
                        }
                        disease_data.append(data)
                        
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"处理疫情报告失败: {e}")
                    continue
            
            if disease_data:
                df = pd.DataFrame(disease_data)
                logger.info(f"成功获取 {len(df)} 条疾病统计数据")
                self._save_data(df, 'disease_statistics')
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取疾病统计数据失败: {e}")
            return pd.DataFrame()
    
    def get_medical_policies(self, year: int = 2024) -> pd.DataFrame:
        """
        获取医疗政策文件
        
        Args:
            year: 年份
            
        Returns:
            政策文件DataFrame
        """
        logger.info(f"开始获取 {year} 年医疗政策文件...")
        
        base_url = "http://www.nhc.gov.cn/wjw/gfxwjj/list.shtml"
        policies = []
        
        try:
            response = self.session.get(base_url, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找政策文件列表
            policy_items = soup.select('ul.zxxx_list li')
            
            for item in policy_items:
                try:
                    link = item.select_one('a')
                    date_span = item.select_one('span.ml')
                    
                    if link and date_span:
                        title = link.text.strip()
                        href = link.get('href', '')
                        date_str = date_span.text.strip()
                        
                        # 筛选指定年份
                        if str(year) in date_str:
                            if not href.startswith('http'):
                                href = f"http://www.nhc.gov.cn{href}"
                            
                            # 提取文号
                            doc_number = re.search(r'〔(\d{4})〕(\d+)号', title)
                            
                            policy = {
                                '政策标题': title,
                                '发布日期': date_str,
                                '文件链接': href,
                                '文号': doc_number.group() if doc_number else '',
                                '年份': year
                            }
                            policies.append(policy)
                            
                except Exception as e:
                    logger.error(f"处理政策项失败: {e}")
                    continue
            
            if policies:
                df = pd.DataFrame(policies)
                logger.info(f"成功获取 {len(df)} 条政策文件")
                self._save_data(df, f'medical_policies_{year}')
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"获取政策文件失败: {e}")
            return pd.DataFrame()
    
    def _get_detail_content(self, url: str, title: str) -> Dict:
        """
        获取详细内容
        
        Args:
            url: 详情页URL
            title: 标题
            
        Returns:
            详细内容字典
        """
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找正文内容
            content_div = soup.select_one('div.con') or soup.select_one('div.content')
            
            if content_div:
                # 提取文本内容
                content_text = content_div.get_text(strip=True, separator='\n')
                
                # 提取可能的统计数据表格
                tables = content_div.select('table')
                table_data = []
                
                for table in tables:
                    try:
                        # 将表格转换为DataFrame
                        table_df = pd.read_html(str(table))[0]
                        table_data.append(table_df.to_dict())
                    except:
                        pass
                
                # 保存原文档
                doc_path = os.path.join(self.save_path, 'documents', f"{title}.txt")
                with open(doc_path, 'w', encoding='utf-8') as f:
                    f.write(content_text)
                
                return {
                    '内容摘要': content_text[:200] + '...' if len(content_text) > 200 else content_text,
                    '全文长度': len(content_text),
                    '包含表格数': len(tables),
                    '表格数据': json.dumps(table_data, ensure_ascii=False) if table_data else ''
                }
            else:
                return {
                    '内容摘要': '',
                    '全文长度': 0,
                    '包含表格数': 0,
                    '表格数据': ''
                }
                
        except Exception as e:
            logger.error(f"获取详细内容失败: {e}")
            return {
                '内容摘要': '',
                '全文长度': 0,
                '包含表格数': 0,
                '表格数据': ''
            }
    
    def _get_disease_detail(self, url: str) -> Dict:
        """
        获取疾病统计详细数据
        
        Args:
            url: 详情页URL
            
        Returns:
            疾病统计数据字典
        """
        try:
            response = self.session.get(url, timeout=10)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 提取疾病统计数据（根据实际页面结构调整）
            disease_stats = {
                '甲类传染病': '',
                '乙类传染病': '',
                '丙类传染病': '',
                '发病数': '',
                '死亡数': ''
            }
            
            # 查找包含统计数据的段落或表格
            content = soup.select_one('div.con')
            if content:
                text = content.get_text()
                
                # 使用正则表达式提取数据
                patterns = {
                    '发病数': r'发病数?(\d+)例',
                    '死亡数': r'死亡(\d+)例',
                    '甲类传染病': r'甲类传染病.*?(\d+)例',
                    '乙类传染病': r'乙类传染病.*?(\d+)例',
                    '丙类传染病': r'丙类传染病.*?(\d+)例'
                }
                
                for key, pattern in patterns.items():
                    match = re.search(pattern, text)
                    if match:
                        disease_stats[key] = match.group(1)
            
            return disease_stats
            
        except Exception as e:
            logger.error(f"获取疾病详细数据失败: {e}")
            return {
                '甲类传染病': '',
                '乙类传染病': '',
                '丙类传染病': '',
                '发病数': '',
                '死亡数': ''
            }
    
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
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='数据')
            
            # 自动调整列宽
            worksheet = writer.sheets['数据']
            for idx, col in enumerate(df.columns):
                max_length = max(
                    df[col].astype(str).apply(len).max(),
                    len(col)
                ) + 2
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 50)
        
        logger.info(f"数据已保存至Excel: {excel_path}")
    
    def run_all(self):
        """
        运行所有爬虫任务
        """
        logger.info("=== 开始执行医疗数据爬虫 ===")
        
        # 1. 获取国家卫健委统计数据
        logger.info("\n1. 爬取国家卫健委统计数据...")
        nhc_data = self.get_nhc_statistics('ylfwygl')
        
        # 2. 获取医院数据
        logger.info("\n2. 爬取医院信息...")
        provinces = ['北京', '上海', '广东']
        hospital_dfs = []
        for province in provinces:
            df = self.get_hospital_data(province)
            if not df.empty:
                hospital_dfs.append(df)
            time.sleep(2)
        
        if hospital_dfs:
            all_hospitals = pd.concat(hospital_dfs, ignore_index=True)
            self._save_data(all_hospitals, 'all_hospitals')
        
        # 3. 获取疾病统计数据
        logger.info("\n3. 爬取疾病统计数据...")
        disease_data = self.get_disease_statistics()
        
        # 4. 获取医疗政策
        logger.info("\n4. 爬取医疗政策文件...")
        policy_data = self.get_medical_policies(2024)
        
        # 统计汇总
        logger.info("\n=== 爬取完成统计 ===")
        logger.info(f"国家卫健委数据: {len(nhc_data) if not nhc_data.empty else 0} 条")
        logger.info(f"医院信息: {len(all_hospitals) if 'all_hospitals' in locals() else 0} 条")
        logger.info(f"疾病统计: {len(disease_data) if not disease_data.empty else 0} 条")
        logger.info(f"政策文件: {len(policy_data) if not policy_data.empty else 0} 条")
        logger.info(f"数据保存路径: {os.path.abspath(self.save_path)}")


def main():
    """
    主函数
    """
    # 创建爬虫实例
    crawler = MedicalDataCrawler()
    
    # 运行所有爬虫任务
    crawler.run_all()
    
    # 也可以单独运行某个任务
    # crawler.get_nhc_statistics('ylfwygl')
    # crawler.get_hospital_data('北京')
    # crawler.get_disease_statistics()
    # crawler.get_medical_policies(2024)


if __name__ == "__main__":
    main()