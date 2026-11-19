#!/usr/bin/env python3
"""
一次性测试脚本：测试1页1条数据，检查字段映射是否正确
"""
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.connection import db_manager
from src.utils.logger import get_logger
import requests
import json

# API配置
API_KEY = "YOUR_TWITTERAPI_IO_KEY"
API_URL = "https://api.twitterapi.io/twitter/user/followings"

logger = get_logger(__name__)

def test_field_mapping():
    """测试字段映射"""

    print("=" * 80)
    print("KOL Following 字段映射测试")
    print("=" * 80)

    # 1. 从数据库获取一个测试KOL
    print("\n1️⃣ 从数据库获取测试KOL...")
    sql = """
    SELECT twitter_user_id as id, username as user_name, name,
           followers_count as followers, following_count as `following`
    FROM public_data.twitter_list_members_seed
    WHERE username IS NOT NULL AND username != ''
        AND following_count > 100 AND following_count < 1000
    ORDER BY following_count DESC
    LIMIT 5
    """

    try:
        kols = db_manager.execute_query(sql)
        if not kols:
            print("❌ 没有找到合适的测试KOL")
            return False

        print(f"✅ 找到 {len(kols)} 个候选KOL")

    except Exception as e:
        print(f"❌ 查询KOL失败: {e}")
        return False

    # 尝试多个KOL，直到找到有数据的
    following = None
    kol = None

    for test_kol in kols:
        print(f"\n尝试KOL: {test_kol['user_name']} (关注数: {test_kol['following']})")

        # 2. 调用API获取1条数据
        try:
            headers = {
                "accept": "application/json",
                "x-api-key": API_KEY
            }

            params = {
                "username": test_kol['user_name'],
                "count": 1  # 只获取1条
            }

            response = requests.get(API_URL, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                print(f"  ⚠️  API调用失败: {response.status_code}")
                continue

            api_data = response.json()

            # 正确的字段是 followings，不是 data.users
            followings = api_data.get('followings', [])

            if not followings:
                print(f"  ⚠️  此KOL无关注数据，尝试下一个...")
                continue

            following = followings[0]  # 只取第一条
            kol = test_kol
            print(f"  ✅ 成功获取数据！")
            break

        except Exception as e:
            print(f"  ❌ API调用失败: {e}")
            continue

    if not following or not kol:
        print("\n❌ 所有测试KOL都无法获取数据")
        return False

    print(f"\n2️⃣ 使用KOL: {kol['user_name']}")

    # 显示API原始字段
    print(f"\n📋 API原始字段:")
    for key in sorted(following.keys()):
        value = following[key]
        if isinstance(value, str) and len(value) > 50:
            value = value[:50] + "..."
        print(f"   {key}: {value}")

    # 3. 映射字段
    print(f"\n3️⃣ 映射数据字段...")

    from datetime import datetime
    from dateutil import parser as date_parser

    try:
        # 解析创建时间
        created_at_str = following.get('created_at')
        created_at_time = None
        account_age_days = None

        if created_at_str:
            try:
                parsed_time = date_parser.parse(created_at_str)
                if parsed_time.tzinfo is not None:
                    created_at_time = parsed_time.replace(tzinfo=None)
                else:
                    created_at_time = parsed_time
                account_age_days = (datetime.now() - created_at_time).days
            except Exception as e:
                print(f"⚠️  解析时间失败: {e}")

        mapped_data = {
            'id': following.get('id'),
            'name': following.get('name'),
            'user_name': following.get('screen_name'),
            'avatar': following.get('profile_image_url_https'),
            'description': following.get('description'),
            'created_at': created_at_str,
            'created_at_time': created_at_time,
            'account_age_days': account_age_days,
            'followers': following.get('followers_count', 0),
            'following': following.get('following_count', 0),
            'statuses_count': following.get('statuses_count', 0),
            'follower_id': kol['user_name'],  # 关注者的user_name
            'update_time': datetime.now()
        }

        print(f"✅ 字段映射完成")
        print(f"\n📊 映射后的数据:")
        for key, value in mapped_data.items():
            if isinstance(value, str) and len(value) > 50:
                value = value[:50] + "..."
            print(f"   {key}: {value}")

    except Exception as e:
        print(f"❌ 字段映射失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 4. 检查数据库表结构
    print(f"\n4️⃣ 检查数据库表结构...")

    try:
        check_sql = "DESCRIBE public_data.twitter_kol_all"
        columns = db_manager.execute_query(check_sql)

        db_fields = [col['Field'] for col in columns]
        print(f"✅ 数据库表有 {len(db_fields)} 个字段")
        print(f"\n数据库字段列表:")
        for field in db_fields:
            print(f"   - {field}")

        # 对比映射字段和数据库字段
        print(f"\n5️⃣ 字段对比分析:")
        mapped_fields = set(mapped_data.keys())
        db_fields_set = set(db_fields)

        missing_in_db = mapped_fields - db_fields_set
        missing_in_mapping = db_fields_set - mapped_fields

        if missing_in_db:
            print(f"❌ 映射中有但数据库没有的字段:")
            for field in sorted(missing_in_db):
                print(f"   - {field}")

        if missing_in_mapping:
            print(f"⚠️  数据库有但映射中缺失的字段:")
            for field in sorted(missing_in_mapping):
                print(f"   - {field}")

        if not missing_in_db and not missing_in_mapping:
            print(f"✅ 所有字段完全匹配！")

    except Exception as e:
        print(f"❌ 检查表结构失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 6. 测试INSERT语句（不实际插入）
    print(f"\n6️⃣ 生成INSERT语句（测试）...")

    try:
        sql = """
        INSERT INTO public_data.twitter_kol_all (
            `id`, `name`, `user_name`, `avatar`, `description`,
            `created_at`, `created_at_time`, `account_age_days`,
            `followers`, `following`, `statuses_count`, `follower_id`, `update_time`
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params = (
            mapped_data['id'],
            mapped_data['name'],
            mapped_data['user_name'],
            mapped_data['avatar'],
            mapped_data['description'],
            mapped_data['created_at'],
            mapped_data['created_at_time'],
            mapped_data['account_age_days'],
            mapped_data['followers'],
            mapped_data['following'],
            mapped_data['statuses_count'],
            mapped_data['follower_id'],
            mapped_data['update_time']
        )

        print(f"✅ INSERT语句构造成功")
        print(f"   字段数: 13")
        print(f"   参数数: {len(params)}")

        if len(params) == 13:
            print(f"✅ 字段数量匹配！")
        else:
            print(f"❌ 字段数量不匹配！")

        # 显示完整的SQL（用于调试）
        print(f"\n📝 INSERT语句预览:")
        print(f"{sql}")
        print(f"\n参数值:")
        field_names = ['id', 'name', 'user_name', 'avatar', 'description',
                      'created_at', 'created_at_time', 'account_age_days',
                      'followers', 'following', 'statuses_count', 'follower_id', 'update_time']
        for i, (field, value) in enumerate(zip(field_names, params)):
            if isinstance(value, str) and len(value) > 50:
                value = str(value)[:50] + "..."
            print(f"   {i+1}. {field}: {value}")

    except Exception as e:
        print(f"❌ 构造INSERT语句失败: {e}")
        import traceback
        traceback.print_exc()
        return False

    # 7. 总结
    print(f"\n" + "=" * 80)
    print(f"✅ 测试完成！")
    print(f"=" * 80)
    print(f"测试KOL: {kol['user_name']}")
    print(f"API调用: 成功")
    print(f"字段映射: 成功")
    print(f"数据库对比: {'完全匹配' if not missing_in_db and not missing_in_mapping else '有差异'}")
    print(f"INSERT语句: {'正确' if len(params) == 13 else '错误'}")
    print(f"\n💡 提示: 此脚本仅测试，未实际写入数据库")
    print(f"=" * 80)

    return True


if __name__ == "__main__":
    try:
        success = test_field_mapping()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  测试被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
