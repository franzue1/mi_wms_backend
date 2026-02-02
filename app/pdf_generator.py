# app/pdf_generator.py
"""
Generador de PDFs para operaciones de inventario.
Compatible con datos de repositorios SQL puros (DictCursor).
Todos los accesos a datos usan .get() para manejo seguro de campos opcionales.
"""

from fpdf import FPDF
from fpdf.enums import XPos, YPos
from datetime import datetime, date
from typing import Dict, Any, List, Optional

from app.database.repositories import operation_repo, partner_repo, warehouse_repo

class PDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 15)
        remission_num = getattr(self, 'remission_number', '')
        title = f'NOTA DE INGRESO: {remission_num}' if 'GR-' not in remission_num else f'GUÍA DE REMISIÓN: {remission_num}'
        self.cell(0, 10, title, 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}', 0, align='C')

def generate_picking_bytes(picking_id: int, company_id: int) -> bytes:
    """
    Genera el PDF de un picking en memoria y devuelve los bytes.
    Compatible con datos de repositorios SQL puros.

    Args:
        picking_id: ID del picking
        company_id: ID de la compañía

    Returns:
        bytes: Contenido del PDF
    """
    picking_info, moves = operation_repo.get_picking_details(picking_id, company_id)
    moves_serials = operation_repo.get_serials_for_picking(picking_id)

    partner_details = None
    partner_id = picking_info.get('partner_id')
    if partner_id:
        partner_details = partner_repo.get_partner_details(partner_id)
    
    pdf = PDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    setattr(pdf, 'remission_number', picking_info.get('remission_number') or 'BORRADOR')
    pdf.add_page()
    
    # --- Info General ---
    pdf.set_font('Helvetica', 'B', 12)
    pdf.cell(0, 8, f"Operación: {picking_info['name']}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 10)

    # Fechas
    fecha_emision_str = "N/A"
    if picking_info.get('date_done'):
        d = picking_info['date_done']
        if isinstance(d, str): 
            try: d = datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
            except: pass
        if isinstance(d, datetime): fecha_emision_str = d.strftime('%d/%m/%Y %H:%M')

    fecha_traslado_str = "N/A"
    if picking_info.get('date_transfer'):
        d = picking_info['date_transfer']
        if isinstance(d, str): 
            try: d = datetime.strptime(d, "%Y-%m-%d").date()
            except: pass
        if isinstance(d, (datetime, date)): fecha_traslado_str = d.strftime('%d/%m/%Y')

    pdf.cell(0, 6, f"Fecha de Registro: {fecha_emision_str}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, f"Fecha de Traslado: {fecha_traslado_str}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, f"Tipo de Operación: {picking_info.get('custom_operation_type') or 'Estándar'}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, f"Doc. Referencia: {picking_info.get('partner_ref') or '-'}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, f"Orden de Compra: {picking_info.get('purchase_order') or '-'}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # Proyecto
    proj_name = picking_info.get('project_name') or "Stock General / Sin Proyecto"
    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(0, 6, f"Proyecto / Obra: {proj_name}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font('Helvetica', '', 10)
    pdf.ln(5)

    # --- Origen/Destino ---
    pdf.set_font('Helvetica', 'B', 10)
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(95, 7, 'PUNTO DE PARTIDA (ORIGEN)', 1, new_x=XPos.RIGHT, new_y=YPos.TOP, align='C', fill=True)
    pdf.cell(95, 7, 'PUNTO DE LLEGADA (DESTINO)', 1, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C', fill=True)
    pdf.set_font('Helvetica', '', 9)

    type_code = picking_info.get('type_code', 'INT')
    
    # [CORRECCIÓN] Usamos warehouse_repo en lugar de location_repo
    def get_addr(loc_id):
        return warehouse_repo.get_location_path(loc_id)

    if type_code == 'IN':
        nom_ori = partner_details['name'] if partner_details else "Proveedor Externo"
        ruc_ori = f"RUC: {partner_details['ruc']}" if partner_details and partner_details.get('ruc') else ""
        dir_ori = partner_details['address'] if partner_details else "-"
    else:
        nom_ori = picking_info.get('warehouse_src_name') or "Almacén Interno"
        ruc_ori = "RUC: 20123456789"
        dir_ori = get_addr(picking_info['location_src_id'])

    if type_code == 'OUT':
        nom_des = partner_details['name'] if partner_details else "Cliente Externo"
        ruc_des = f"RUC: {partner_details['ruc']}" if partner_details and partner_details.get('ruc') else ""
        dir_des = partner_details['address'] if partner_details else "-"
    else:
        nom_des = picking_info.get('warehouse_dest_name') or "Almacén Interno"
        ruc_des = "RUC: 20123456789"
        dir_des = get_addr(picking_info['location_dest_id'])

    pdf.cell(95, 6, str(nom_ori)[:50], 'LR', new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.cell(95, 6, str(nom_des)[:50], 'LR', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(95, 6, str(ruc_ori), 'LR', new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.cell(95, 6, str(ruc_des), 'LR', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(95, 6, f"Dir: {str(dir_ori)[:55]}", 'LRB', new_x=XPos.RIGHT, new_y=YPos.TOP)
    pdf.cell(95, 6, f"Dir: {str(dir_des)[:55]}", 'LRB', new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(10)

    # --- Tabla Productos ---
    pdf.set_font('Helvetica', 'B', 9)
    pdf.set_fill_color(240, 240, 240)
    w_sku, w_desc, w_qty, w_uom = 30, 120, 20, 20
    pdf.cell(w_sku, 8, 'CÓDIGO', 1, 0, 'C', True)
    pdf.cell(w_desc, 8, 'DESCRIPCIÓN / SERIES', 1, 0, 'L', True)
    pdf.cell(w_uom, 8, 'UND', 1, 0, 'C', True)
    pdf.cell(w_qty, 8, 'CANT', 1, 1, 'C', True)
    pdf.set_font('Helvetica', '', 9)

    for move in moves:
        line_height = 5
        # Acceso seguro con .get() para todos los campos
        move_name = move.get('name', '')
        move_id = move.get('id')
        move_sku = move.get('sku', '')
        move_uom = move.get('uom_name', 'Und')
        move_qty = move.get('quantity_done', 0)

        desc_text = move_name
        serials_data = moves_serials.get(move_id, {})
        if serials_data:
            series_list = list(serials_data.keys())
            series_str = ", ".join(series_list)
            desc_text += f"\n [SN: {series_str}]"

        x_start, y_start = pdf.get_x(), pdf.get_y()

        pdf.set_xy(x_start + w_sku, y_start)
        pdf.multi_cell(w_desc, line_height, desc_text, border=1, align='L')
        y_end = pdf.get_y()
        row_height = y_end - y_start

        pdf.set_xy(x_start, y_start)
        pdf.cell(w_sku, row_height, str(move_sku), border=1, align='C')
        pdf.set_xy(x_start + w_sku + w_desc, y_start)
        pdf.cell(w_uom, row_height, str(move_uom), border=1, align='C')
        pdf.set_xy(x_start + w_sku + w_desc + w_uom, y_start)
        qty_str = f"{int(move_qty)}" if move_qty % 1 == 0 else f"{move_qty:.2f}"
        pdf.cell(w_qty, row_height, qty_str, border=1, align='C')
        pdf.set_y(y_end)

    # --- Firma ---
    pdf.ln(30)
    y_sig = pdf.get_y()
    if y_sig > 250: 
        pdf.add_page(); y_sig = pdf.get_y() + 20
    pdf.line(20, y_sig, 80, y_sig); pdf.line(130, y_sig, 190, y_sig)
    pdf.text(25, y_sig + 5, "Entregado por"); pdf.text(135, y_sig + 5, "Recibido por")

    return pdf.output()