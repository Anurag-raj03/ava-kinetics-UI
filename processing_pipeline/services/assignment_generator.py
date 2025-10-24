import random
from typing import List, Dict
from collections import defaultdict

class AssignmentGenerator:
    """
    Distributes a list of items (keyframes) among annotators, ensuring a
    specified percentage of items are assigned to a second person for overlap.
    """
    def __init__(self):
        pass

    def generate_random_assignments(
        self,
        items: List[str],
        annotators: List[str],
        overlap_percentage: int
    ) -> Dict[str, List[str]]:
        """
        Generates a random assignment plan.

        Args:
            items: A list of all available item filenames (e.g., ['keyframe1.jpg', 'keyframe2.jpg']).
            annotators: A list of annotator usernames.
            overlap_percentage: The percentage of items that should be assigned to a second annotator.

        Returns:
            A dictionary mapping annotator usernames to their assigned items.
        """
        if not items or not annotators:
            raise ValueError("Items and annotators lists cannot be empty.")

        num_annotators = len(annotators)
        num_items = len(items)
        random.shuffle(items)

        assignments = defaultdict(list)

        # Step 1: Distribute all items to a primary annotator (round-robin)
        for i, item in enumerate(items):
            primary_annotator = annotators[i % num_annotators]
            assignments[primary_annotator].append(item)

        # Step 2: Handle overlap
        # Convert percentage to a number of items
        num_overlap_items = int(num_items * (overlap_percentage / 100))
        # Randomly sample items to be used for overlap
        overlap_items = random.sample(items, k=min(num_overlap_items, num_items))

        for item in overlap_items:
            # Find who was originally assigned this item
            primary_annotator = next(
                (ann for ann, assigned in assignments.items() if item in assigned), None
            )
            
            # Find all other annotators who could be assigned this for overlap
            available_overlap_annotators = [ann for ann in annotators if ann != primary_annotator]

            if available_overlap_annotators:
                # Pick one of them randomly
                overlap_annotator = random.choice(available_overlap_annotators)
                # Add the item to their assignment list
                assignments[overlap_annotator].append(item)

        # Return a regular dictionary with the lists of items sorted for consistency
        return {ann: sorted(item_list) for ann, item_list in assignments.items()}