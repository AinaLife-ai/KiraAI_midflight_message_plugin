"""Minimal stand-in for KiraAI's ``core`` package.

These tests drive ``main.py`` exactly the way the framework does, but without
needing a running KiraAI instance.  Only the surface actually touched by the
plugin is stubbed: the plugin hooks (``on``), ``Priority``, ``BasePlugin``,
``logger``, and the two message types it imports.
"""
