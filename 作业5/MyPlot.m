%******************************************************** 
%   说明：运行时需要指定数据集名称、类别总数、类标签所在列数
%   DatasetName  数据集路径，可以读取part-m-00000或者txt
%   CNUM         类别总数
%   CD=3;        类标签所在列
%*******************************************************
close all
clear all
clc

DatasetName='/Users/shijianjun/学习资料/金融大数据处理/作业/作业5/5_10/part-m-00000'; % 路径修改
CNUM=5;  % 类别总数 类别不同要修改
CD=3; % 类标签所在维数(第几列)

data=load(DatasetName);  % 加载数据
shape=['*' '+' 'x' 'd' 's' 'v' 'p' 'h' '.' 'o' 'v' '<']; % 默认12个形状
cmap=colormap;
for i=1:CNUM
    icmp=int8((i*64.)/(CNUM*1.)); %取颜色
    cate_data=data(find(data(:,CD)==i),:);  % 找到第i类的全部数据
    is=rem(i,12); %取形状
    if is==0
        is=is+1;
    end
    hold on
    plot(cate_data(:,1),cate_data(:,2),shape(is),'MarkerSize',5,'MarkerFaceColor',cmap(icmp,:),'MarkerEdgeColor',cmap(icmp,:));  % MarkerSize指定“点大小”
end
box on
axis equal
