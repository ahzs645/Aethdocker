from app import create_app

app, port = create_app()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port, debug=True)
