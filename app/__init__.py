from flask import Flask
import os

def create_app():
    # Get port from environment variable with default of 5000
    port = int(os.environ.get('PORT', 5000))
    
    app = Flask(__name__)
    app.config['UPLOAD_FOLDER'] = 'data'
    app.config['RESULTS_FOLDER'] = 'data/results'
    app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024  # 1GB max upload size
    
    # Create folders if they don't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    # Import and register blueprints
    from app.routes.main_routes import main_bp
    from app.routes.api_routes import api_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    
    return app, port
