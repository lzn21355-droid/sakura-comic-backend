import json
import datetime
import requests
from sqlalchemy import func
from ..extensions import db
from ..utils.avalon_logger import logger
from ..models.models import MovType, MovInfo, MovDetail


class SakuarDataSchedule:
    def __init__(self):
        self.sakura_list = 'https://m3u8.apiyhzy.com/api.php/provide/vod/?ac=list&pg={page}'
        self.sakura_detail = 'https://m3u8.apiyhzy.com/api.php/provide/vod/?ac=detail&pg={page}'
        self.total_page = None
        self.avalon_latest_time = None
        self.stop_craw = False  # 当此值为True 不继续抓取数据

    @staticmethod
    def get_avalon_latest_time() -> datetime.datetime:
        '''
        获取数据库内 数据最新更新时间，处理空表场景
        :return:
        '''
        avalon_latest_time = db.session.query(func.max(MovDetail.vod_time)).scalar()
        if not avalon_latest_time:
            # 数据库为空时，返回极小时间，确保后续数据能插入
            return datetime.datetime.strptime('2000-01-01', '%Y-%m-%d')
        # 仅取日期部分，避免时间精度导致的漏数据
        avalon_latest_time_str = avalon_latest_time.strftime('%Y-%m-%d %H:%M:%S').split(' ')[0]
        return datetime.datetime.strptime(avalon_latest_time_str, '%Y-%m-%d')

    def insert_or_update_movdetail(self, mov_list: list) -> None:
        '''
        将 movdetail 数据 插入或更新到数据库，处理字段类型
        :param mov_list: list
        :return:
        '''
        need_insert_mov_list = []
        for mov_detail in mov_list:
            # 字段类型转换：字符串转整数/日期
            int_fields = ['vod_hits', 'vod_hits_day', 'vod_hits_week', 'vod_hits_month', 
                          'vod_year', 'type_id', 'vod_total', 'vod_score_num']
            for field in int_fields:
                if field in mov_detail and mov_detail[field] is not None:
                    try:
                        mov_detail[field] = int(mov_detail[field])
                    except (ValueError, TypeError):
                        mov_detail[field] = 0  # 转换失败设默认值
            # 日期字段转换
            if 'vod_time' in mov_detail and mov_detail['vod_time']:
                try:
                    mov_detail['vod_time'] = datetime.datetime.strptime(mov_detail['vod_time'], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    mov_detail['vod_time'] = datetime.datetime.now()  # 格式错误设当前时间

            vod_time = mov_detail.get('vod_time', datetime.datetime.now())
            if vod_time > self.avalon_latest_time:
                self.stop_craw = False
                vod_id = mov_detail['vod_id']
                avalon_mov_detail = MovDetail.query.filter_by(vod_id=vod_id).first()
                if avalon_mov_detail:
                    # 数据库已有此数据 执行更新操作
                    for k, v in mov_detail.items():
                        setattr(avalon_mov_detail, k, v)
                    db.session.commit()
                else:
                    # 数据库没有此数据 执行插入操作
                    need_insert_mov_list.append(mov_detail)
            else:
                self.stop_craw = True

        if need_insert_mov_list:
            #  如果有需要插入的新数据
            try:
                db.session.bulk_insert_mappings(MovDetail, need_insert_mov_list)
                db.session.commit()
                logger.info(f"批量插入{len(need_insert_mov_list)}条新数据")
            except Exception as e:
                db.session.rollback()
                logger.error(f"批量插入失败：{str(e)}")

    def get_sakura_data(self) -> None:
        '''
        获取樱花数据 对已有数据进行更新操作 其他执行插入操作
        :return: None
        '''
        self.avalon_latest_time = self.get_avalon_latest_time()  # 数据库vod_time最大值

        url = self.sakura_detail.format(page=1)
        logger.info(f'Updating: {url}')
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = json.loads(response.text)
                total = int(data['total'])
                limit_per_page = int(data['limit'])
                self.total_page = (total // limit_per_page) + 1 if total % limit_per_page else total // limit_per_page
                mov_detail_list = data['list']
                self.insert_or_update_movdetail(mov_detail_list)
            else:
                logger.debug(f"抓取数据失败, code: {response.status_code}")
        except Exception as e:
            logger.error(f"第1页请求失败：{str(e)}")

        for i in range(2, self.total_page + 1):  # 修正循环范围，包含最后一页
            if self.stop_craw:
                logger.info("数据已更新完毕: 停止抓取")
                break
            url = self.sakura_detail.format(page=i)
            logger.info(f'Updating: {url}')
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    data = json.loads(response.text)
                    mov_detail_list = data['list']
                    self.insert_or_update_movdetail(mov_detail_list)
                else:
                    logger.debug(f"抓取数据失败, code: {response.status_code}")
            except Exception as e:
                logger.error(f"第{i}页请求失败：{str(e)}")


class SakuraData:
    def __init__(self):
        self.total_page = None
        self.mov_type = None
        self.sakura_list = 'https://m3u8.apiyhzy.com/api.php/provide/vod/?ac=list&pg={page}'
        self.sakura_detail = 'https://m3u8.apiyhzy.com/api.php/provide/vod/?ac=detail&pg={page}'
        self.__init_sakura__()

    def __init_sakura__(self):
        try:
            url = self.sakura_list.format(page=1)
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = json.loads(response.text)
                if self.mov_type is None:
                    self.mov_type = data['class']
                if self.total_page is None:
                    total = int(data['total'])
                    limit_per_page = int(data['limit'])
                    self.total_page = (total // limit_per_page) + 1 if total % limit_per_page else total // limit_per_page
            else:
                raise Exception(f"初始化失败: 无法获取数据，HTTP code: {response.status_code}")
        except Exception as e:
            logger.error(f"初始化失败: {str(e)}")
            raise  # 抛出异常，让调用方感知

    def insert_mov_type(self):
        try:
            db.session.bulk_insert_mappings(MovType, self.mov_type)
            db.session.commit()
            logger.info(f'保存视频类型进入数据库，共{len(self.mov_type)}条')
        except Exception as e:
            db.session.rollback()
            logger.error(f"插入视频类型失败：{str(e)}")

    def get_mov_info(self, page=1):
        # 读取url数据并保存到数据库，处理字段类型
        url = self.sakura_list.format(page=page)
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = json.loads(response.text)
                mov_info_list = data['list']
                # 字段类型转换（根据MovInfo模型补充）
                for item in mov_info_list:
                    int_fields = ['type_id', 'vod_id', 'vod_total']
                    for field in int_fields:
                        if field in item and item[field] is not None:
                            try:
                                item[field] = int(item[field])
                            except (ValueError, TypeError):
                                item[field] = 0
                db.session.bulk_insert_mappings(MovInfo, mov_info_list)
                db.session.commit()
                logger.info(f'mov_list page {page} catched (数据量：{len(mov_info_list)})')
            else:
                logger.debug(f"抓取数据失败, code: {response.status_code}")
        except Exception as e:
            db.session.rollback()
            logger.error(f'插入mov_info page {page} 失败：{str(e)}')

    def get_mov_detail(self, page=1):
        '''
        抓取 mov detail，处理字段类型
        :param page: 页码
        :return:
        '''
        url = self.sakura_detail.format(page=page)
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                data = json.loads(response.text)
                mov_detail_list = data['list']
                # 字段类型转换
                for item in mov_detail_list:
                    int_fields = ['vod_hits', 'vod_hits_day', 'vod_hits_week', 'vod_hits_month', 
                                  'vod_year', 'type_id', 'vod_total', 'vod_score_num']
                    for field in int_fields:
                        if field in item and item[field] is not None:
                            try:
                                item[field] = int(item[field])
                            except (ValueError, TypeError):
                                item[field] = 0
                db.session.bulk_insert_mappings(MovDetail, mov_detail_list)
                db.session.commit()
                logger.info(f'mov_detail page {page} catched (数据量：{len(mov_detail_list)})')
            else:
                logger.debug(f"抓取数据失败, code: {response.status_code}")
        except Exception as e:
            db.session.rollback()
            logger.error(f'插入mov_detail page {page} 失败：{str(e)}')

    def crawl_mov_info_all(self):
        # 修正循环范围：从1到total_page（包含）
        for page in range(1, self.total_page + 1):
            self.get_mov_info(page)

    def crawl_mov_detail_all(self):
        for page in range(1, self.total_page + 1):
            self.get_mov_detail(page)