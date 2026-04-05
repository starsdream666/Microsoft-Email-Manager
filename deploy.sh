#!/bin/bash

# Microsoft-Email-Manager - 一键部署脚本
# 作者: AI Assistant
# 描述: 自动化Docker部署流程

set -e

echo "🚀 Microsoft-Email-Manager - 一键部署脚本"
echo "======================================="

# 检查Docker和docker-compose是否安装
check_dependencies() {
    echo "📋 检查依赖..."
    
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker未安装，请先安装Docker"
        echo "   安装指南: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ docker-compose未安装，请先安装docker-compose"
        echo "   安装指南: https://docs.docker.com/compose/install/"
        exit 1
    fi
    
    echo "✅ 依赖检查通过"
}

# 创建必要的目录
create_directories() {
    echo "📁 创建数据目录..."
    mkdir -p data
    
    # 创建空的accounts.json如果不存在
    if [ ! -f "accounts.json" ]; then
        echo "{}" > accounts.json
        echo "✅ 创建空的账户配置文件"
    fi
}

# 构建和启动服务
deploy_service() {
    echo "🔨 构建Docker镜像..."
    docker-compose build
    
    echo "🚀 启动服务..."
    docker-compose up -d
    
    echo "⏳ 等待服务启动..."
    sleep 10
    
    # 检查服务状态
    if docker-compose ps | grep -q "Up"; then
        echo "✅ 服务启动成功！"
        echo ""
        echo "📋 服务信息:"
        echo "   - Web界面: http://localhost:8000"
        echo "   - API文档: http://localhost:8000/docs"
        echo "   - 服务状态: docker-compose ps"
        echo "   - 查看日志: docker-compose logs -f"
        echo ""
        echo "🎉 部署完成！"
    else
        echo "❌ 服务启动失败，请检查日志:"
        echo "   docker-compose logs"
        exit 1
    fi
}

# 显示管理命令
show_management_commands() {
    echo ""
    echo "🛠️  常用管理命令:"
    echo "   启动服务: docker-compose up -d"
    echo "   停止服务: docker-compose down"
    echo "   重启服务: docker-compose restart"
    echo "   查看日志: docker-compose logs -f"
    echo "   查看状态: docker-compose ps"
    echo ""
}

# 主流程
main() {
    check_dependencies
    create_directories
    deploy_service
    show_management_commands
}

# 捕获中断信号
trap 'echo "❌ 部署中断"; exit 1' INT

# 执行主流程
main

echo "✨ 感谢使用 Microsoft-Email-Manager!"
