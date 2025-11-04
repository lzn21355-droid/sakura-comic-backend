import sys
import os
import requests
from requests.exceptions import RequestException
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.factory import create_app
from app.task.tasks import SakuraData
from app.extensions import db
from app.models.models import MovType

class SakuraCrawler:
    def __init__(self):
        self.app = create_app()
        
    def init_mov_types(self):
        """初始化视频类型"""
        with self.app.app_context():
            try:
                existing_types = MovType.query.first()
                if existing_types:
                    print("✅ 视频类型已存在，跳过初始化")
                    return True
                else:
                    print("🔄 初始化视频类型...")
                    sd = SakuraData()
                    sd.insert_mov_type()
                    print("✅ 视频类型初始化完成")
                    return True
            except Exception as e:
                print(f"❌ 视频类型初始化失败: {str(e)}")
                return False
    
    def crawl_pages(self, start_page=1, end_page=5, max_retries=3, delay=2):
        """爬取指定范围的页面"""
        with self.app.app_context():
            print(f"🚀 开始爬取第 {start_page} 到 {end_page} 页数据")
            
            success_count = 0
            fail_count = 0
            
            for page in range(start_page, end_page + 1):
                for attempt in range(max_retries):
                    try:
                        print(f"📄 正在爬取第 {page} 页 (尝试 {attempt + 1}/{max_retries})...")
                        sd = SakuraData()
                        sd.get_mov_detail(page)
                        print(f"✅ 第 {page} 页爬取完成")
                        success_count += 1
                        break
                    except RequestException as e:
                        print(f"❌ 第 {page} 页网络请求失败: {str(e)}")
                        if attempt < max_retries - 1:
                            print(f"⏳ 等待 {delay} 秒后重试...")
                            time.sleep(delay)
                        else:
                            print(f"💥 第 {page} 页爬取失败")
                            fail_count += 1
                    except Exception as e:
                        db.session.rollback()
                        print(f"❌ 第 {page} 页处理失败: {str(e)}")
                        if attempt < max_retries - 1:
                            print(f"⏳ 等待 {delay} 秒后重试...")
                            time.sleep(delay)
                        else:
                            print(f"💥 第 {page} 页爬取失败")
                            fail_count += 1
            
            print(f"🎉 爬取完成！成功: {success_count} 页, 失败: {fail_count} 页")
    
    def full_crawl(self):
        """全量爬取"""
        with self.app.app_context():
            print("🚀 开始全量爬取...")
            sd = SakuraData()
            sd.crawl_mov_detail_all()
            print("🎉 全量爬取完成！")

def main():
    """主函数"""
    crawler = SakuraCrawler()
    
    # 初始化视频类型
    if crawler.init_mov_types():
        # 选择爬取方式
        print("请选择爬取方式:")
        print("1. 测试爬取 (5页)")
        print("2. 全量爬取")
        print("3. 自定义范围爬取")
        
        choice = input("请输入选择 (1/2/3): ").strip()
        
        if choice == "1":
            crawler.crawl_pages(1, 5)
        elif choice == "2":
            crawler.full_crawl()
        elif choice == "3":
            start = int(input("起始页码: "))
            end = int(input("结束页码: "))
            crawler.crawl_pages(start, end)
        else:
            print("无效选择，使用默认测试爬取")
            crawler.crawl_pages(1, 5)
    else:
        print("⚠️ 视频类型初始化失败，尝试直接爬取...")
        crawler.full_crawl()

if __name__ == '__main__':
    main()