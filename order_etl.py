"""
订单数据ETL处理程序
根据需求文档对Excel订单数据进行转换处理
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse


def calculate_fiscal_fields(date_val):
    """
    根据回团日期计算自然月、财月、财季、财年
    财年从自然月6月开始，自然月6月为财月1月，自然月6、7、8为财季1
    """
    if pd.isna(date_val):
        return None, None, None, None
    
    if isinstance(date_val, str):
        date_obj = datetime.strptime(str(date_val)[:10], "%Y-%m-%d")
    else:
        date_obj = date_val
    
    natural_month = date_obj.month  # 自然月
    
    # 计算财月：自然月6月为财月1月
    if natural_month >= 6:
        fiscal_month = natural_month - 5  # 6->1, 7->2, ..., 12->7
        fiscal_year = date_obj.year + 1   # 财年 = 自然年 + 1
    else:
        fiscal_month = natural_month + 7  # 1->8, 2->9, ..., 5->12
        fiscal_year = date_obj.year       # 财年 = 自然年
    
    # 计算财季：每3个月一个季度
    fiscal_quarter = (fiscal_month - 1) // 3 + 1  # 1-3->Q1, 4-6->Q2, 7-9->Q3, 10-12->Q4
    
    return natural_month, fiscal_month, f"Q{fiscal_quarter}", f"FY{str(fiscal_year)[-2:]}"


def get_income_category(row):
    """
    需求2：构造"收入大类"字段
    1. 团队所属公司包含"云南"且订单所属公司包含"云南" -> 销售收入&产品收入
    2. 团队所属公司包含"云南" -> 产品收入
    3. 订单所属公司包含"云南"或"国际游学" -> 销售收入
    """
    team_company = str(row.get("团队所属公司", ""))
    order_company = str(row.get("订单所属公司", ""))
    
    if "云南" in team_company and "云南" in order_company:
        return "销售收入&产品收入"
    elif "云南" in team_company:
        return "产品收入"
    elif "云南" in order_company or "国际游学" in order_company:
        return "销售收入"
    
    return ""


def get_income_subcategory(row):
    """
    需求3：构造"收入细分"字段
    1. 团队所属公司含云南 且 订单所属公司含云南 -> 自研自销（100%）
    2. 团队所属公司含云南 且 订单所属公司不含云南 -> 他销
    3. 团队所属公司不含云南 且 (订单所属公司含云南 或 国际游学) -> 代销（100%）
    """
    team_company = str(row.get("团队所属公司", ""))
    order_company = str(row.get("订单所属公司", ""))
    
    team_has_yunnan = "云南" in team_company
    order_has_yunnan = "云南" in order_company
    order_has_international = "国际游学" in order_company
    
    if team_has_yunnan and order_has_yunnan:
        return "自研自销（100%）"
    elif team_has_yunnan and not order_has_yunnan:
        return "他销"
    elif not team_has_yunnan and (order_has_yunnan or order_has_international):
        return "代销（100%）"
    
    return ""


def get_budget_project_category(row):
    """
    需求4：构造"项目分类集团预算口径"字段
    直接取"产品类别"的值
    """
    return row.get("产品类别", "")


def get_ynwlz_project_category(row):
    """
    需求5：构造"项目分类云南文旅口径"字段
    根据复杂的规则判断返回对应值
    """
    product_category = str(row.get("产品类别", ""))
    team_full_name = str(row.get("团队全称", ""))
    order_type = str(row.get("订单类型", ""))
    
    # 规则1：产品类别包含"国际游学" -> 国际游学
    if "国际游学" in product_category:
        return "国际游学"
    
    # 规则2：产品类别包含"国际文旅" 且 团队全称不包含"大游"或"学校" -> 国际文旅散客
    if "国际文旅" in product_category:
        if "大游" not in team_full_name and "学校" not in team_full_name:
            return "国际文旅散客"
        
        # 规则3：产品类别包含"国际文旅" 且 团队全称包含"大游"或"学校" -> 国际员工大游
        if "大游" in team_full_name or "学校" in team_full_name:
            return "国际员工大游"
    
    # 规则4：产品类别包含"营地教育" -> 营地教育
    if "营地教育" in product_category:
        return "营地教育"
    
    # 规则5-7、19：国内研学相关
    if "国内研学" in product_category:
        if "定制" in team_full_name:
            return "研学渠道"
        elif "营" in team_full_name:
            # 规则19：产品类别包含"国内研学"且团队全称包含"营" -> 研学独立
            return "研学独立"
        elif "独立" in team_full_name:
            return "研学独立"
        elif "亲子" in team_full_name:
            return "研学亲子"
    
    # 规则8-9：国内亲子/国内文旅 - 野趣野判断
    if "国内亲子" in product_category or "国内文旅" in product_category:
        if "野趣野" in team_full_name:
            # 规则8：产品类别含"国内亲子"或"国内文旅"且团队全称含"野趣野" -> 研学亲子
            return "研学亲子"
    
    # 规则9：产品类别包含"国内亲子"且不包含"野趣野" -> 文旅亲子
    if "国内亲子" in product_category:
        return "文旅亲子"
    
    # 规则10-17：国内文旅相关
    if "国内文旅" in product_category:
        # 规则10：包含"大游"或"学校" -> 国内员工大游
        if "大游" in team_full_name or "学校" in team_full_name:
            return "国内员工大游"
        
        # 规则11：订单类型包含"内部" -> 内部定制
        if "内部" in order_type:
            return "内部定制"
        
        # 规则12：订单类型包含"外部" -> 外部定制
        if "外部" in order_type:
            return "外部定制"
        
        # 规则13：团队全称包含"学校定制"且订单类型包含"散客" -> 内部定制
        if "学校定制" in team_full_name and "散客" in order_type:
            return "内部定制"
        
        # 规则14：团队全称包含"定制"且订单类型包含"散客" -> 外部定制
        if "定制" in team_full_name and "散客" in order_type:
            return "外部定制"
        
        # 规则15：团队全称包含"昆明号" -> 列车散客
        if "昆明号" in team_full_name:
            return "列车散客"
        
        # 规则16：团队全称包含"房车" -> 房车散客
        if "房车" in team_full_name:
            return "房车散客"
        
        # 规则17：不包含上述关键词 -> 文旅亲子
        if ("定制" not in team_full_name and 
            "学校" not in team_full_name and 
            "列车" not in team_full_name and 
            "房车" not in team_full_name):
            return "文旅亲子"
    
    # 规则18：产品类别包含"中老年"且团队全称不包含"定制"或"学校"或"列车"或"房车" -> 文旅亲子
    if "中老年" in product_category:
        if ("定制" not in team_full_name and 
            "学校" not in team_full_name and 
            "列车" not in team_full_name and 
            "房车" not in team_full_name):
            return "文旅亲子"
    
    return "研学独立"


def process_order_data(input_file: str, output_file: str = None):
    """
    处理订单数据的主函数
    """
    print(f"正在读取文件: {input_file}")
    
    # 读取Excel文件
    df = pd.read_excel(input_file)
    print(f"读取成功，共 {len(df)} 条记录")
    
    # 应用需求1：根据回团日期计算自然月、财月、财季、财年
    print("正在处理需求1：计算财政日期字段...")
    df["自然月"], df["财月"], df["财季"], df["财年"] = zip(
        *df["回团日期"].apply(calculate_fiscal_fields)
    )
    
    # 应用需求2：构造收入大类
    print("正在处理需求2：构造收入大类...")
    df["收入大类"] = df.apply(get_income_category, axis=1)
    
    # 应用需求3：构造收入细分
    print("正在处理需求3：构造收入细分...")
    df["收入细分"] = df.apply(get_income_subcategory, axis=1)
    
    # 应用需求4：构造项目分类集团预算口径
    print("正在处理需求4：构造项目分类集团预算口径...")
    df["项目分类集团预算口径"] = df.apply(get_budget_project_category, axis=1)
    
    # 应用需求5：构造项目分类云南文旅口径
    print("正在处理需求5：构造项目分类云南文旅口径...")
    df["项目分类云南文旅口径"] = df.apply(get_ynwlz_project_category, axis=1)
    
    # 需求6：调整列顺序，将新增字段放在前面
    new_columns = [
        "自然月", "财年", "财季", "财月",
        "收入大类", "收入细分",
        "项目分类集团预算口径", "项目分类云南文旅口径"
    ]
    
    # 获取原表的所有列
    original_columns = [col for col in df.columns if col not in new_columns]
    
    # 按照需求6的顺序排列列
    final_columns = new_columns + original_columns
    df = df[final_columns]
    
    # 输出文件
    if output_file is None:
        # 生成输出文件名，路径与输入文件一致
        input_path = Path(input_file)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = str(input_path.parent / f"转换结果表_{timestamp}.xlsx")
    
    print(f"正在输出文件: {output_file}")
    df.to_excel(output_file, index=False)
    
    print(f"\n处理完成！")
    print(f"- 总记录数: {len(df)}")
    print(f"- 新增字段: {', '.join(new_columns)}")
    print(f"- 输出文件: {output_file}")
    
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="订单数据ETL处理程序")
    parser.add_argument("-i", "--input", dest="input_excel", default="导出数据.xlsx", help="输入Excel文件路径（默认: 导出数据.xlsx）")
    parser.add_argument("-o", "--output", dest="output_file", default=None, help="输出Excel文件路径（默认与输入文件同目录）")
    args = parser.parse_args()

    # 执行处理
    result_df = process_order_data(args.input_excel, args.output_file)
    
    # 显示前几条数据预览
    print("\n--- 数据预览(前5条) ---")
    preview_cols = ["自然月", "财年", "财季", "财月", "收入大类", "收入细分", 
                    "项目分类集团预算口径", "项目分类云南文旅口径", "团号", "回团日期"]
    print(result_df[preview_cols].head().to_string())
