#!/usr/bin/env python3
"""
XML to Database Converter

This script converts FDA SPL XML documents to typed Python dataclass objects
and saves the structured data to a PostgreSQL database.
"""

import argparse
import sys
import os
from pathlib import Path
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import connection as Connection
import logging
from datetime import datetime

from spl_xml_to_dataclass import SPLXMLToDataclassConverter
from spl_data_types import (
    SPLDocument, ManufacturedProduct, Ingredient, IngredientSubstance,
    ActiveMoiety, Packaging, Approval, MarketingAct, Characteristic,
    ObservationMedia
)


class DatabaseConfig:
    """Database connection configuration."""
    
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "fda_spls", user: str = "postgres", 
                 password: str = "postgres"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    def get_connection_string(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


class SPLDatabaseInserter:
    """Handles insertion of SPL data into PostgreSQL database."""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
        self.connection: Optional[Connection] = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(self.db_config.get_connection_string())
            self.connection.autocommit = False  # Use transactions
            self.logger.info("Connected to database successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from database")
    
    def insert_spl_document(self, spl_doc: SPLDocument) -> bool:
        """Insert complete SPL document into database."""
        if not self.connection:
            self.logger.error("No database connection")
            return False
        
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            # Start transaction
            self.logger.info(f"Starting import of document: {spl_doc.document.id.root}")
            
            # 1. Insert or get organization
            org_id = self._insert_organization(cursor, spl_doc)
            
            # 2. Insert document
            doc_id = self._insert_document(cursor, spl_doc, org_id)
            if not doc_id:
                raise Exception("Failed to insert document")
            
            # 3. Insert products and related data
            for product in spl_doc.manufactured_products:
                product_id = self._insert_product(cursor, product, doc_id)
                if product_id:
                    self._insert_product_data(cursor, product, product_id)
            
            # 4. Insert observation media
            for media in spl_doc.observation_media:
                self._insert_observation_media(cursor, media, doc_id)
            
            # Commit transaction
            self.connection.commit()
            cursor.close()
            
            self.logger.info(f"Successfully imported document: {spl_doc.document.id.root}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error importing document: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def _insert_organization(self, cursor, spl_doc: SPLDocument) -> Optional[int]:
        """Insert or get organization ID."""
        if not spl_doc.author or not spl_doc.author.assignedEntity:
            return None
        
        org = spl_doc.author.assignedEntity.representedOrganization
        if not org or not org.name:
            return None
        
        # Check if organization exists
        cursor.execute("""
            SELECT id FROM organizations 
            WHERE id_root = %s AND id_extension = %s
        """, (org.id.root if org.id else None, 
              org.id.extension if org.id else None))
        
        result = cursor.fetchone()
        if result:
            return result['id']
        
        # Insert new organization
        cursor.execute("""
            INSERT INTO organizations (name, id_root, id_extension)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (org.name, 
              org.id.root if org.id else None,
              org.id.extension if org.id else None))
        
        result = cursor.fetchone()
        return result['id'] if result else None
    
    def _insert_document(self, cursor, spl_doc: SPLDocument, org_id: Optional[int]) -> Optional[int]:
        """Insert document record."""
        doc = spl_doc.document
        
        # Parse effective time
        effective_time = None
        if doc.effectiveTime and doc.effectiveTime.value:
            try:
                # HL7 date format: YYYYMMDD
                effective_time = datetime.strptime(doc.effectiveTime.value, '%Y%m%d').date()
            except ValueError:
                self.logger.warning(f"Could not parse effective time: {doc.effectiveTime.value}")
        
        cursor.execute("""
            INSERT INTO documents (
                document_id_root, code, code_system, code_display_name,
                title_text, title_html, effective_time, set_id_root,
                version_number, author_organization_id, source_file_path
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            doc.id.root,
            doc.code.code if doc.code else None,
            doc.code.codeSystem if doc.code else None,
            doc.code.displayName if doc.code else None,
            doc.title.text if doc.title else None,
            doc.title.html if doc.title else None,
            effective_time,
            doc.setId.root if doc.setId else None,
            int(doc.versionNumber.value) if doc.versionNumber and doc.versionNumber.value else None,
            org_id,
            spl_doc.source_file.path if spl_doc.source_file else None
        ))
        
        result = cursor.fetchone()
        return result['id'] if result else None
    
    def _insert_product(self, cursor, product: ManufacturedProduct, doc_id: int) -> Optional[int]:
        """Insert product record."""
        # Extract route information
        route_code = None
        route_code_system = None
        route_display_name = None
        
        if product.consumedIn and product.consumedIn.substanceAdministration:
            route = product.consumedIn.substanceAdministration.routeCode
            if route:
                route_code = route.code
                route_code_system = route.codeSystem
                route_display_name = route.displayName
        
        cursor.execute("""
            INSERT INTO products (
                document_id, product_code, product_code_system,
                name_text, name_html, name_suffix,
                form_code, form_code_system, form_display_name,
                route_code, route_code_system, route_display_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            doc_id,
            product.code.code if product.code else None,
            product.code.codeSystem if product.code else None,
            product.name.text if product.name else None,
            product.name.html if product.name else None,
            product.name.suffix if product.name else None,
            product.formCode.code if product.formCode else None,
            product.formCode.codeSystem if product.formCode else None,
            product.formCode.displayName if product.formCode else None,
            route_code,
            route_code_system,
            route_display_name
        ))
        
        result = cursor.fetchone()
        return result['id'] if result else None
    
    def _insert_product_data(self, cursor, product: ManufacturedProduct, product_id: int):
        """Insert all product-related data."""
        # Generic medicines
        for generic_name in product.genericMedicine:
            cursor.execute("""
                INSERT INTO generic_medicines (product_id, generic_name)
                VALUES (%s, %s)
            """, (product_id, generic_name))
        
        # Ingredients
        for ingredient in product.ingredients:
            self._insert_ingredient(cursor, ingredient, product_id)
        
        # Packaging
        for package in product.packaging:
            self._insert_packaging(cursor, package, product_id)
        
        # Subject of data
        if product.subjectOf:
            # Approvals
            for approval in product.subjectOf.approvals:
                self._insert_approval(cursor, approval, product_id)
            
            # Marketing acts
            for marketing_act in product.subjectOf.marketingActs:
                self._insert_marketing_act(cursor, marketing_act, product_id)
            
            # Characteristics
            for characteristic in product.subjectOf.characteristics:
                self._insert_characteristic(cursor, characteristic, product_id)
    
    def _insert_ingredient(self, cursor, ingredient: Ingredient, product_id: int):
        """Insert ingredient and related substance data."""
        if not ingredient.ingredientSubstance:
            return
        
        # Insert or get ingredient substance
        substance_id = self._insert_ingredient_substance(cursor, ingredient.ingredientSubstance)
        if not substance_id:
            return
        
        # Extract quantity information
        num_value = None
        num_unit = None
        denom_value = None
        denom_unit = None
        
        if ingredient.quantity:
            if ingredient.quantity.numerator:
                num_value = float(ingredient.quantity.numerator.value) if ingredient.quantity.numerator.value else None
                num_unit = ingredient.quantity.numerator.unit
            if ingredient.quantity.denominator:
                denom_value = float(ingredient.quantity.denominator.value) if ingredient.quantity.denominator.value else None
                denom_unit = ingredient.quantity.denominator.unit
        
        cursor.execute("""
            INSERT INTO ingredients (
                product_id, ingredient_substance_id, class_code,
                quantity_numerator_value, quantity_numerator_unit,
                quantity_denominator_value, quantity_denominator_unit
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            product_id, substance_id, ingredient.classCode,
            num_value, num_unit, denom_value, denom_unit
        ))
    
    def _insert_ingredient_substance(self, cursor, substance: IngredientSubstance) -> Optional[int]:
        """Insert or get ingredient substance."""
        if not substance.name:
            return None
        
        # Check if substance exists
        cursor.execute("""
            SELECT id FROM ingredient_substances 
            WHERE substance_code = %s AND substance_code_system = %s
        """, (substance.code.code if substance.code else None,
              substance.code.codeSystem if substance.code else None))
        
        result = cursor.fetchone()
        if result:
            substance_id = result['id']
        else:
            # Insert new substance
            cursor.execute("""
                INSERT INTO ingredient_substances (substance_code, substance_code_system, substance_name)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (substance.code.code if substance.code else None,
                  substance.code.codeSystem if substance.code else None,
                  substance.name))
            
            result = cursor.fetchone()
            substance_id = result['id'] if result else None
        
        # Insert active moieties
        if substance_id and substance.activeMoiety:
            for moiety in substance.activeMoiety:
                cursor.execute("""
                    INSERT INTO active_moieties (
                        ingredient_substance_id, moiety_code, moiety_code_system, moiety_name
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    substance_id,
                    moiety.code.code if moiety.code else None,
                    moiety.code.codeSystem if moiety.code else None,
                    moiety.name
                ))
        
        return substance_id
    
    def _insert_packaging(self, cursor, package: Packaging, product_id: int):
        """Insert packaging configuration."""
        # Extract quantity and container information
        num_value = None
        num_unit = None
        denom_value = None
        translation_code = None
        translation_code_system = None
        translation_display_name = None
        
        if package.quantity:
            if package.quantity.numerator:
                num_value = float(package.quantity.numerator.value) if package.quantity.numerator.value else None
                num_unit = package.quantity.numerator.unit
                if package.quantity.numerator.translation:
                    translation_code = package.quantity.numerator.translation.code
                    translation_code_system = package.quantity.numerator.translation.codeSystem
                    translation_display_name = package.quantity.numerator.translation.displayName
            
            if package.quantity.denominator:
                denom_value = float(package.quantity.denominator.value) if package.quantity.denominator.value else None
        
        # Container information
        container_code = None
        container_code_system = None
        container_form_code = None
        container_form_code_system = None
        container_form_display_name = None
        
        if package.containerPackagedProduct:
            container_code = package.containerPackagedProduct.code
            container_code_system = package.containerPackagedProduct.codeSystem
            if package.containerPackagedProduct.formCode:
                container_form_code = package.containerPackagedProduct.formCode.code
                container_form_code_system = package.containerPackagedProduct.formCode.codeSystem
                container_form_display_name = package.containerPackagedProduct.formCode.displayName
        
        cursor.execute("""
            INSERT INTO packaging_configurations (
                product_id, quantity_numerator_value, quantity_numerator_unit,
                quantity_denominator_value, translation_code, translation_code_system,
                translation_display_name, container_code, container_code_system,
                container_form_code, container_form_code_system, container_form_display_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            product_id, num_value, num_unit, denom_value,
            translation_code, translation_code_system, translation_display_name,
            container_code, container_code_system,
            container_form_code, container_form_code_system, container_form_display_name
        ))
    
    def _insert_approval(self, cursor, approval: Approval, product_id: int):
        """Insert approval record."""
        # Extract territory information
        territory_code = None
        territory_code_system = None
        
        if approval.author and approval.author.territorialAuthority:
            territory = approval.author.territorialAuthority.territory
            if territory:
                territory_code = territory.code
                territory_code_system = territory.codeSystem
        
        cursor.execute("""
            INSERT INTO approvals (
                product_id, approval_id_root, approval_id_extension,
                approval_code, approval_code_system, approval_display_name,
                territory_code, territory_code_system
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            product_id,
            approval.id.root if approval.id else None,
            approval.id.extension if approval.id else None,
            approval.code.code if approval.code else None,
            approval.code.codeSystem if approval.code else None,
            approval.code.displayName if approval.code else None,
            territory_code,
            territory_code_system
        ))
    
    def _insert_marketing_act(self, cursor, marketing_act: MarketingAct, product_id: int):
        """Insert marketing act record."""
        # Parse effective time
        effective_time_low = None
        effective_time_high = None
        
        if marketing_act.effectiveTime:
            if marketing_act.effectiveTime.low and marketing_act.effectiveTime.low.value:
                try:
                    effective_time_low = datetime.strptime(marketing_act.effectiveTime.low.value, '%Y%m%d').date()
                except ValueError:
                    pass
            
            if marketing_act.effectiveTime.value:
                try:
                    effective_time_high = datetime.strptime(marketing_act.effectiveTime.value, '%Y%m%d').date()
                except ValueError:
                    pass
        
        cursor.execute("""
            INSERT INTO marketing_acts (
                product_id, marketing_code, marketing_code_system,
                status_code, effective_time_low, effective_time_high
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            product_id,
            marketing_act.code.code if marketing_act.code else None,
            marketing_act.code.codeSystem if marketing_act.code else None,
            marketing_act.statusCode.code if marketing_act.statusCode else None,
            effective_time_low,
            effective_time_high
        ))
    
    def _insert_characteristic(self, cursor, characteristic: Characteristic, product_id: int):
        """Insert product characteristic."""
        # Extract value information based on type
        value_type = None
        value_code = None
        value_code_system = None
        value_display_name = None
        value_text = None
        value_numeric = None
        value_unit = None
        
        if characteristic.value:
            value_type = characteristic.value.xsi_type
            
            if value_type == 'CE':
                value_code = characteristic.value.code
                value_code_system = characteristic.value.codeSystem
                value_display_name = characteristic.value.displayName
            elif value_type in ['PQ', 'INT']:
                if characteristic.value.value:
                    try:
                        value_numeric = float(characteristic.value.value)
                    except (ValueError, TypeError):
                        pass
                value_unit = characteristic.value.unit
            elif value_type == 'ST':
                value_text = characteristic.value.text
        
        cursor.execute("""
            INSERT INTO product_characteristics (
                product_id, characteristic_code, characteristic_code_system,
                value_type, value_code, value_code_system, value_display_name,
                value_text, value_numeric, value_unit
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            product_id,
            characteristic.code.code if characteristic.code else None,
            characteristic.code.codeSystem if characteristic.code else None,
            value_type, value_code, value_code_system, value_display_name,
            value_text, value_numeric, value_unit
        ))
    
    def _insert_observation_media(self, cursor, media: ObservationMedia, doc_id: int):
        """Insert observation media record."""
        media_type = None
        media_reference = None
        
        if media.value:
            media_type = media.value.mediaType
            media_reference = media.value.reference
        
        cursor.execute("""
            INSERT INTO observation_media (
                document_id, media_id, media_text, media_type, media_reference
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            doc_id, media.ID, media.text, media_type, media_reference
        ))


def main():
    """Main function to handle command line arguments and run conversion."""
    parser = argparse.ArgumentParser(description='Convert FDA SPL XML to database')
    parser.add_argument('xml_file', help='Input XML file path')
    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', type=int, default=5432, help='Database port (default: 5432)')
    parser.add_argument('--database', default='fda_spls', help='Database name (default: fda_spls)')
    parser.add_argument('--user', default='postgres', help='Database user (default: postgres)')
    parser.add_argument('--password', default='postgres', help='Database password (default: postgres)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # Validate input file
    if not Path(args.xml_file).exists():
        logger.error(f"Input file '{args.xml_file}' not found.")
        sys.exit(1)
    
    try:
        # Convert XML to dataclass
        logger.info(f"Converting XML file: {args.xml_file}")
        converter = SPLXMLToDataclassConverter(args.xml_file)
        spl_document = converter.convert()
        logger.info(f"Converted document with {len(spl_document.manufactured_products)} products")
        
        # Setup database connection
        db_config = DatabaseConfig(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password
        )
        
        # Insert into database
        inserter = SPLDatabaseInserter(db_config)
        if not inserter.connect():
            logger.error("Failed to connect to database")
            sys.exit(1)
        
        try:
            success = inserter.insert_spl_document(spl_document)
            if success:
                logger.info("Successfully inserted SPL document into database")
                print("SUCCESS: Document imported to database")
            else:
                logger.error("Failed to insert SPL document")
                sys.exit(1)
        finally:
            inserter.disconnect()
    
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()