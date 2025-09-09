"""
Duplicate detection utilities for SPL documents.
Identifies potential duplicate products within documents.
"""

from typing import List, Dict, Set, Tuple, Optional
import re
from difflib import SequenceMatcher


class DuplicateDetector:
    """Detects potential duplicate products within SPL documents."""
    
    # Similarity thresholds
    NAME_SIMILARITY_THRESHOLD = 0.85
    INGREDIENT_SIMILARITY_THRESHOLD = 0.90
    OVERALL_SIMILARITY_THRESHOLD = 0.80
    
    # Words to ignore when comparing product names
    IGNORE_WORDS = {
        'tablet', 'capsule', 'injection', 'solution', 'suspension', 'cream', 'ointment',
        'gel', 'lotion', 'spray', 'inhaler', 'drops', 'extended', 'release', 'immediate',
        'delayed', 'controlled', 'sustained', 'modified', 'mg', 'g', 'ml', 'mcg', 'ug',
        'generic', 'brand', 'oral', 'topical', 'ophthalmic', 'otic', 'rectal',
    }
    
    @classmethod
    def find_duplicates(cls, products: List[Dict]) -> List[Dict]:
        """
        Find potential duplicate products in a list.
        
        Args:
            products: List of product dictionaries with keys like 'name', 'ingredients', etc.
            
        Returns:
            list: List of duplicate groups with similarity scores
        """
        if len(products) < 2:
            return []
        
        duplicates = []
        processed_indices = set()
        
        for i in range(len(products)):
            if i in processed_indices:
                continue
                
            current_group = [i]
            
            for j in range(i + 1, len(products)):
                if j in processed_indices:
                    continue
                
                similarity = cls._calculate_product_similarity(products[i], products[j])
                
                if similarity >= cls.OVERALL_SIMILARITY_THRESHOLD:
                    current_group.append(j)
                    processed_indices.add(j)
            
            if len(current_group) > 1:
                duplicate_group = {
                    'indices': current_group,
                    'products': [products[idx] for idx in current_group],
                    'similarity_scores': {},
                    'confidence': 'high' if len(current_group) > 2 else 'medium'
                }
                
                # Calculate pairwise similarities within group
                for idx1 in current_group:
                    for idx2 in current_group:
                        if idx1 < idx2:
                            sim_score = cls._calculate_product_similarity(products[idx1], products[idx2])
                            duplicate_group['similarity_scores'][f"{idx1}-{idx2}"] = sim_score
                
                duplicates.append(duplicate_group)
                processed_indices.update(current_group)
        
        return duplicates
    
    @classmethod
    def _calculate_product_similarity(cls, product1: Dict, product2: Dict) -> float:
        """Calculate overall similarity between two products."""
        similarities = []
        weights = []
        
        # Name similarity (high weight)
        name_sim = cls._calculate_name_similarity(
            product1.get('name', ''), 
            product2.get('name', '')
        )
        similarities.append(name_sim)
        weights.append(0.4)
        
        # Ingredient similarity (high weight)
        ingredient_sim = cls._calculate_ingredient_similarity(
            product1.get('ingredients', []), 
            product2.get('ingredients', [])
        )
        similarities.append(ingredient_sim)
        weights.append(0.4)
        
        # Dosage form similarity (medium weight)
        form_sim = cls._calculate_form_similarity(
            product1.get('dosage_form', ''), 
            product2.get('dosage_form', '')
        )
        similarities.append(form_sim)
        weights.append(0.1)
        
        # Route similarity (medium weight)
        route_sim = cls._calculate_route_similarity(
            product1.get('route', ''), 
            product2.get('route', '')
        )
        similarities.append(route_sim)
        weights.append(0.1)
        
        # Calculate weighted average
        weighted_sum = sum(sim * weight for sim, weight in zip(similarities, weights))
        total_weight = sum(weights)
        
        return weighted_sum / total_weight if total_weight > 0 else 0.0
    
    @classmethod
    def _calculate_name_similarity(cls, name1: str, name2: str) -> float:
        """Calculate similarity between product names."""
        if not name1 or not name2:
            return 0.0
        
        # Normalize names
        norm_name1 = cls._normalize_product_name(name1)
        norm_name2 = cls._normalize_product_name(name2)
        
        if not norm_name1 or not norm_name2:
            return 0.0
        
        # Exact match
        if norm_name1 == norm_name2:
            return 1.0
        
        # Sequence similarity
        return SequenceMatcher(None, norm_name1, norm_name2).ratio()
    
    @classmethod
    def _normalize_product_name(cls, name: str) -> str:
        """Normalize product name for comparison."""
        if not name:
            return ""
        
        # Convert to lowercase
        normalized = name.lower()
        
        # Remove common pharmaceutical terms
        words = normalized.split()
        filtered_words = [word for word in words if word not in cls.IGNORE_WORDS]
        
        # Remove numbers and special characters for core comparison
        core_name = re.sub(r'[\d\W]+', ' ', ' '.join(filtered_words))
        
        # Normalize whitespace
        return ' '.join(core_name.split())
    
    @classmethod
    def _calculate_ingredient_similarity(cls, ingredients1: List[Dict], ingredients2: List[Dict]) -> float:
        """Calculate similarity between ingredient lists."""
        if not ingredients1 and not ingredients2:
            return 1.0
        
        if not ingredients1 or not ingredients2:
            return 0.0
        
        # Extract active ingredient names
        active1 = cls._extract_active_ingredients(ingredients1)
        active2 = cls._extract_active_ingredients(ingredients2)
        
        if not active1 and not active2:
            return 1.0
        
        if not active1 or not active2:
            return 0.0
        
        # Calculate Jaccard similarity
        set1 = set(active1)
        set2 = set(active2)
        
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    @classmethod
    def _extract_active_ingredients(cls, ingredients: List[Dict]) -> List[str]:
        """Extract active ingredient names from ingredient list."""
        active_ingredients = []
        
        for ingredient in ingredients:
            if not isinstance(ingredient, dict):
                continue
            
            # Check if it's an active ingredient
            class_code = ingredient.get('class_code', '').upper()
            if class_code and 'ACTIB' not in class_code:  # Not active ingredient
                continue
            
            # Extract ingredient name
            ingredient_substance = ingredient.get('ingredient_substance', {})
            if isinstance(ingredient_substance, dict):
                name = ingredient_substance.get('name', '')
                if name:
                    # Normalize ingredient name
                    normalized_name = re.sub(r'[\W\d]+', ' ', name.lower()).strip()
                    if normalized_name:
                        active_ingredients.append(normalized_name)
        
        return active_ingredients
    
    @classmethod
    def _calculate_form_similarity(cls, form1: str, form2: str) -> float:
        """Calculate similarity between dosage forms."""
        if not form1 and not form2:
            return 1.0
        
        if not form1 or not form2:
            return 0.0
        
        # Normalize forms
        norm_form1 = form1.lower().strip()
        norm_form2 = form2.lower().strip()
        
        # Exact match
        if norm_form1 == norm_form2:
            return 1.0
        
        # Check for base form match (ignore modifiers like "extended_release")
        base_form1 = norm_form1.split('_')[0]
        base_form2 = norm_form2.split('_')[0]
        
        if base_form1 == base_form2:
            return 0.8  # High similarity but not exact
        
        # Partial similarity
        return SequenceMatcher(None, norm_form1, norm_form2).ratio()
    
    @classmethod
    def _calculate_route_similarity(cls, route1: str, route2: str) -> float:
        """Calculate similarity between routes of administration."""
        if not route1 and not route2:
            return 1.0
        
        if not route1 or not route2:
            return 0.0
        
        # Normalize routes
        norm_route1 = route1.lower().strip()
        norm_route2 = route2.lower().strip()
        
        # Exact match
        if norm_route1 == norm_route2:
            return 1.0
        
        # Check for related routes
        related_routes = [
            {'oral', 'buccal', 'sublingual'},
            {'intravenous', 'intramuscular', 'subcutaneous'},
            {'topical', 'transdermal'},
            {'ophthalmic', 'otic'},
        ]
        
        for related_group in related_routes:
            if norm_route1 in related_group and norm_route2 in related_group:
                return 0.7  # Related but not identical
        
        return 0.0
    
    @classmethod
    def analyze_duplicate_confidence(cls, duplicate_group: Dict) -> str:
        """Analyze confidence level of duplicate detection."""
        products = duplicate_group.get('products', [])
        similarities = duplicate_group.get('similarity_scores', {})
        
        if len(products) < 2:
            return 'none'
        
        # Calculate average similarity
        avg_similarity = sum(similarities.values()) / len(similarities) if similarities else 0
        
        # High confidence criteria
        if avg_similarity >= 0.95:
            return 'very_high'
        elif avg_similarity >= 0.90:
            return 'high'
        elif avg_similarity >= 0.80:
            return 'medium'
        elif avg_similarity >= 0.70:
            return 'low'
        else:
            return 'very_low'
    
    @classmethod
    def get_deduplication_recommendation(cls, duplicate_group: Dict) -> Dict[str, any]:
        """Get recommendation for handling detected duplicates."""
        confidence = cls.analyze_duplicate_confidence(duplicate_group)
        products = duplicate_group.get('products', [])
        
        recommendation = {
            'action': 'manual_review',
            'confidence': confidence,
            'reason': '',
            'suggested_primary': None,
            'suggested_action': 'keep_all'
        }
        
        if confidence in ['very_high', 'high']:
            recommendation['action'] = 'merge_or_remove'
            recommendation['reason'] = 'High similarity suggests true duplicates'
            
            # Suggest keeping the most complete record
            most_complete_idx = cls._find_most_complete_product(products)
            if most_complete_idx is not None:
                recommendation['suggested_primary'] = most_complete_idx
                recommendation['suggested_action'] = 'keep_primary'
        
        elif confidence == 'medium':
            recommendation['action'] = 'flag_for_review'
            recommendation['reason'] = 'Moderate similarity requires manual verification'
        
        else:
            recommendation['action'] = 'keep_all'
            recommendation['reason'] = 'Low similarity - likely different products'
        
        return recommendation
    
    @classmethod
    def _find_most_complete_product(cls, products: List[Dict]) -> Optional[int]:
        """Find the most complete product record in a group."""
        if not products:
            return None
        
        completeness_scores = []
        
        for product in products:
            score = 0
            
            # Award points for having various fields
            if product.get('name'):
                score += 2
            if product.get('ingredients'):
                score += 3
            if product.get('dosage_form'):
                score += 1
            if product.get('route'):
                score += 1
            if product.get('strength'):
                score += 2
            if product.get('ndc_codes'):
                score += 2
            
            completeness_scores.append(score)
        
        # Return index of most complete product
        max_score = max(completeness_scores)
        return completeness_scores.index(max_score) if max_score > 0 else None