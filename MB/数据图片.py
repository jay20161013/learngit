# -*- coding:utf-8 -*-
__author__ = 'YangXin'
import sys
import pandas as pd
import codecs
import imgkit
reload(sys)
sys.setdefaultencoding('utf-8')


# ReportImage -> report convert include multiple sheets into pictures
class ReportImage:

    def __init__(self):
        pass

    # excel_html -> convert excel include multiple sheets into multiple html file
    # excel_file -> file
    # html_path  -> path
    @staticmethod
    def excel_html(excel_file, html_path):
        html_list = []
        excel_obj = pd.ExcelFile(excel_file)
        sheet_list = excel_obj.sheet_names
        index = 0
        for i in sheet_list:
            html_file = html_path + i + ".html"
            excel_data = excel_obj.parse(excel_obj.sheet_names[index])
            with codecs.open(html_file, 'w', 'utf-8') as html:
                html.write(excel_data.to_html(header=True, index=True))
            html_list.append(html_file)
            index += 1
        return html_list

    # html_image -> convert htmls into pictures file
    # html_list  -> list
    # image_path -> path
    @staticmethod
    def html_image(html_list, image_path):
        index = 0
        for i in html_list:
            img_obj = image_path + str(index) + ".png"
            with open(i, 'r') as html_file:
                imgkit.from_file(html_file, img_obj, options={"encoding":"UTF-8"})
            index += 1


if __name__ == '__main__':
    html_list = ReportImage.excel_html("/xxx.xlsx", "/yyy/")
    ReportImage.html_image(html_list, "/zzz/")
