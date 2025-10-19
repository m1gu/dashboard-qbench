*** Begin Patch
*** Update File: qbench_dashboard/ui/main_window.py
@@
-        except (RuntimeError, AttributeError):
-            series = QHorizontalBarSeries()
-            chart.addSeries(series)
-            self.test_types_series = series
+        except (RuntimeError, AttributeError):
+            series = QHorizontalBarSeries()
+            series.setLabelsVisible(False)
+            chart.addSeries(series)
+            self.test_types_series = series
*** End Patch
