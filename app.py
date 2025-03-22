import os
import json
from flask import Flask, request, jsonify, session
from flask_cors import CORS
from werkzeug.utils import secure_filename
import uuid
from datetime import datetime
from pdf_processor import PDFToStructuredData

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.secret_key = os.environ.get('SECRET_KEY', 'development-key-change-in-production')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed_data'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size

# Ensure upload and processed folders exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

@app.route('/api/upload', methods=['POST'])  # Added /api prefix
def upload_file():
    """Handle PDF upload and processing"""
    if 'pdf_file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
        
    file = request.files['pdf_file']
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
        
    if file and file.filename.lower().endswith('.pdf'):
        # Generate unique filename
        unique_id = uuid.uuid4().hex
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = secure_filename(f"{unique_id}_{timestamp}_{file.filename}")
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        try:
            # Save the uploaded file
            file.save(file_path)
            
            # Process the PDF with our backend tool
            processor = PDFToStructuredData(output_dir=app.config['PROCESSED_FOLDER'])
            result = processor.process_pdf(file_path)
            
            if 'error' in result:
                return jsonify({'error': result['error']}), 500
                
            # Get the path to the JSON file
            json_path = result['output_files'].get('json')
            
            if not json_path or not os.path.exists(json_path):
                return jsonify({'error': 'Failed to process PDF'}), 500
                
            # Store the result path in session
            session['current_data_path'] = json_path
            
            # Return success with file ID for frontend to use
            return jsonify({
                'success': True,
                'message': f'File processed successfully. Found {len(result.get("structured_data", {}).get("structured_data", {}).get("items", []))} items.',
                'data_path': json_path
            })
            
        except Exception as e:
            return jsonify({'error': f'Error processing file: {str(e)}'}), 500
    
    return jsonify({'error': 'Invalid file format. Please upload a PDF file.'}), 400

@app.route('/api/get-data', methods=['GET'])  # Added /api prefix
def get_data():
    """Return processed data for display"""
    data_path = session.get('current_data_path')
    
    if not data_path or not os.path.exists(data_path):
        return jsonify({'error': 'No processed data available'}), 404
        
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Get current shortlist
        shortlist = session.get('shortlist', [])
            
        return jsonify({
            'success': True,
            'data': data,
            'shortlist': shortlist
        })
        
    except Exception as e:
        return jsonify({'error': f'Error loading data: {str(e)}'}), 500

@app.route('/api/shortlist', methods=['POST'])  # Added /api prefix
def update_shortlist():
    """Add or remove an item from the shortlist"""
    data = request.json
    item_id = data.get('item_id')
    action = data.get('action')  # 'add' or 'remove'
    
    if not item_id:
        return jsonify({'error': 'No item ID provided'}), 400
        
    # Get current shortlist
    shortlist = session.get('shortlist', [])
    
    if action == 'add' and item_id not in shortlist:
        shortlist.append(item_id)
    elif action == 'remove' and item_id in shortlist:
        shortlist.remove(item_id)
        
    # Update session
    session['shortlist'] = shortlist
    
    return jsonify({
        'success': True,
        'shortlist': shortlist
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)