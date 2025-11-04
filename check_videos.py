# check_videos.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.factory import create_app
from app.models.models import MovieDetail

def check_video_data():
    app = create_app()
    with app.app_context():
        # 统计视频数据
        total_videos = MovieDetail.query.count()
        videos_with_url = MovieDetail.query.filter(MovieDetail.play_url != None).count()
        videos_with_vod_url = MovieDetail.query.filter(MovieDetail.vod_play_url != None).count()
        
        print(f"总视频数: {total_videos}")
        print(f"有播放地址的视频: {videos_with_url}")
        print(f"有VOD播放地址的视频: {videos_with_vod_url}")
        
        # 检查前10条数据的播放地址
        print("\n前10条视频数据:")
        videos = MovieDetail.query.limit(10).all()
        for i, video in enumerate(videos, 1):
            print(f"{i}. 标题: {video.title}")
            print(f"   播放地址: {video.play_url}")
            print(f"   VOD地址: {video.vod_play_url}")
            print()

if __name__ == '__main__':
    check_video_data()