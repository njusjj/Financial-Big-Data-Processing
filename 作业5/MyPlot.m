%******************************************************** 
%   ˵��������ʱ��Ҫָ�����ݼ����ơ�������������ǩ��������
%   DatasetName  ���ݼ�·�������Զ�ȡpart-m-00000����txt
%   CNUM         �������
%   CD=3;        ���ǩ������
%*******************************************************
close all
clear all
clc

DatasetName='/Users/shijianjun/ѧϰ����/���ڴ����ݴ���/��ҵ/��ҵ5/5_10/part-m-00000'; % ·���޸�
CNUM=5;  % ������� ���ͬҪ�޸�
CD=3; % ���ǩ����ά��(�ڼ���)

data=load(DatasetName);  % ��������
shape=['*' '+' 'x' 'd' 's' 'v' 'p' 'h' '.' 'o' 'v' '<']; % Ĭ��12����״
cmap=colormap;
for i=1:CNUM
    icmp=int8((i*64.)/(CNUM*1.)); %ȡ��ɫ
    cate_data=data(find(data(:,CD)==i),:);  % �ҵ���i���ȫ������
    is=rem(i,12); %ȡ��״
    if is==0
        is=is+1;
    end
    hold on
    plot(cate_data(:,1),cate_data(:,2),shape(is),'MarkerSize',5,'MarkerFaceColor',cmap(icmp,:),'MarkerEdgeColor',cmap(icmp,:));  % MarkerSizeָ�������С��
end
box on
axis equal
